use hbsync_sim::{Action, Event, Sim};
use hbsync_types::NodeId;
use proptest::prelude::*;

fn action() -> impl Strategy<Value = Action> {
    let i = || 0usize..16;
    prop_oneof![
        3 => (i(), i()).prop_map(|(parent, name)| Action::CreateFile { parent, name }),
        2 => (i(), i()).prop_map(|(parent, name)| Action::CreateFolder { parent, name }),
        3 => i().prop_map(|file| Action::Edit { file }),
        2 => (i(), i(), i()).prop_map(|(node, parent, name)| Action::Move { node, parent, name }),
        1 => i().prop_map(|node| Action::Delete { node }),
        1 => i().prop_map(|parent| Action::CreateJunk { parent }),
        1 => Just(Action::ToggleIgnore),
        1 => i().prop_map(|node| Action::Restore { node }),
        1 => Just(Action::Purge),
        1 => Just(Action::ListDeleted),
        1 => Just(Action::EmptyListed),
        1 => i().prop_map(|pick| Action::EmptyItem { pick }),
        1 => i().prop_map(|pick| Action::EditTaken { pick }),
        2 => (i(), i()).prop_map(|(node, tag)| Action::Tag { node, tag }),
        1 => (i(), i()).prop_map(|(node, tag)| Action::Colour { node, tag }),
        1 => (i(), i()).prop_map(|(op, pick)| Action::Phone { op, pick }),
        1 => Just(Action::DeleteRoleFolder),
        1 => (i(), i()).prop_map(|(pick, done)| Action::DeleteStopped { pick, done }),
        1 => i().prop_map(|file| Action::MoveAcross { file }),
    ]
}

fn event(clients: usize) -> impl Strategy<Value = Event> {
    let c = move || 0..clients;
    prop_oneof![
        4 => (c(), action()).prop_map(|(client, action)| Event::Act { client, action }),
        1 => c().prop_map(|client| Event::Plan { client }),
        3 => c().prop_map(|client| Event::Step { client }),
    ]
}

/// Events with the user confirming held deletes now and then.
fn guarded_event(clients: usize) -> impl Strategy<Value = Event> {
    prop_oneof![
        8 => event(clients),
        1 => (0..clients).prop_map(|client| Event::Confirm { client }),
    ]
}

/// Events with a client dying now and then ([`Event::Crash`]).
fn crashing_event(clients: usize) -> impl Strategy<Value = Event> {
    prop_oneof![
        8 => event(clients),
        1 => (0..clients).prop_map(|client| Event::Crash { client }),
    ]
}

fn run(clients: usize, events: &[Event]) -> Result<(), TestCaseError> {
    run_with(clients, None, |sim| sim, events)
}

/// Runs `events` with the mass-delete guard at `guard` (its minimum), on a simulation `setup`
/// prepares, then lets every client settle (confirming what the guard holds, with the guard
/// on) and checks the outcome.
fn run_with(
    clients: usize,
    guard: Option<usize>,
    setup: fn(Sim) -> Sim,
    events: &[Event],
) -> Result<(), TestCaseError> {
    run_settled(clients, guard, setup, |sim| sim.quiesce(30), events)
}

/// [`run_with`], the clients settling as `settle` lets them (whether they did).
fn run_settled(
    clients: usize,
    guard: Option<usize>,
    setup: fn(Sim) -> Sim,
    settle: fn(&mut Sim) -> bool,
    events: &[Event],
) -> Result<(), TestCaseError> {
    let outcome = |verbose: bool| {
        let mut sim = setup(Sim::new(clients));
        sim.guard = guard;
        sim.verbose = verbose;
        sim.run(events);
        let result = if settle(&mut sim) {
            sim.check()
        } else {
            Err("no convergence".into())
        };
        (result, sim.trace)
    };
    if let (Err(e), _) = outcome(false) {
        // Replay with tracing so the (minimal) failure explains itself.
        let (_, trace) = outcome(true);
        let keep = if std::env::var("SIM_FULL_TRACE").is_ok() {
            usize::MAX
        } else {
            40
        };
        let tail = trace.len().saturating_sub(keep);
        return Err(TestCaseError::fail(format!(
            "{e}\n--- trace ---\n{}",
            trace[tail..].join("\n")
        )));
    }
    Ok(())
}

proptest! {
    // 2000 cases, unless `PROPTEST_CASES` says otherwise (a set `cases` would override it).
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(2000),
        ..ProptestConfig::default()
    })]

    #[test]
    fn two_clients_converge_without_loss(events in proptest::collection::vec(event(2), 0..80)) {
        run(2, &events)?;
    }

    #[test]
    fn three_clients_converge_without_loss(events in proptest::collection::vec(event(3), 0..120)) {
        run(3, &events)?;
    }

    /// No poll (daemon.md): clients that sync only when asked (at start, when another changed
    /// the drive, when the user confirms what the guard held) converge without loss, and then
    /// a sync nobody asked for finds nothing to do: a timer never would.
    #[test]
    fn clients_converge_when_asked_only(
        events in proptest::collection::vec(guarded_event(3), 0..120),
        min in 1usize..4,
    ) {
        run_settled(3, Some(min), |sim| sim, |sim| sim.quiesce_announced(300), &events)?;
    }

    /// With the mass-delete guard on (a low limit, so it trips often), no plan deletes more
    /// than it allows unless the user confirmed those very deletes, and once the user confirms
    /// what is held, the clients still converge without loss.
    #[test]
    fn guarded_clients_delete_only_what_was_confirmed(
        events in proptest::collection::vec(guarded_event(2), 0..80),
        min in 1usize..3,
    ) {
        run_with(2, Some(min), |sim| sim, &events)?;
    }

    /// The main folders leave a role folder another device made alone (§12): never synced
    /// here, never changed, whatever the users do around it.
    #[test]
    fn main_folders_leave_role_folders_alone(events in proptest::collection::vec(event(3), 0..120)) {
        run_with(3, None, Sim::with_role_folder, &events)?;
    }

    /// One computer syncs the main folder and the role folder, each in its own coordinates,
    /// beside two computers with the main folder only, while the phone works in the role
    /// folder (§12): each folder converges to its part of the drive and writes nowhere else,
    /// a node is in one folder of a computer at most, files moved between the folders and
    /// between the parts are never lost.
    #[test]
    fn role_folder_devices_converge_without_loss(events in proptest::collection::vec(event(4), 0..120)) {
        run_with(3, None, Sim::with_role_device, &events)?;
    }

    /// Two computers add Documents, which the drive doesn't have yet: the first to sync makes
    /// it, the other joins it, merging what both hold, while the users work in both folders of
    /// both computers and the phone works in it once it is there (plan E1).
    #[test]
    fn new_role_folder_devices_converge_without_loss(events in proptest::collection::vec(event(5), 0..120)) {
        run_with(3, None, Sim::with_new_role_devices, &events)?;
    }

    /// A download fetches, then puts in place and commits a step later: meanwhile the drive
    /// moves on or removes the node, the users work, the folder's other device syncs; and a
    /// client dies between a download put in place and its commit, recovering at its next
    /// sync. Each commit, and each recovered one, settles against the version it acted on with
    /// the bytes it fetched: the clients converge without loss.
    #[test]
    fn downloads_that_race_the_drive_or_crash_converge_without_loss(
        events in proptest::collection::vec(crashing_event(3), 0..120),
    ) {
        run_with(3, None, Sim::with_split_downloads, &events)?;
    }
}

#[test]
fn concurrent_edit_produces_a_conflict_copy() {
    let mut sim = Sim::new(2);
    let create = Action::CreateFile { parent: 0, name: 0 };
    sim.run(&[Event::Act {
        client: 0,
        action: create,
    }]);
    assert!(sim.quiesce(10));
    sim.run(&[
        Event::Act {
            client: 0,
            action: Action::Edit { file: 0 },
        },
        Event::Act {
            client: 1,
            action: Action::Edit { file: 0 },
        },
    ]);
    assert!(sim.quiesce(10));
    assert_eq!(sim.check(), Ok(()));
    assert!(sim.conflicts > 0);
}

#[test]
fn regression_three_clients_case_clash() {
    use Action::*;
    let events = [
        Event::Act {
            client: 1,
            action: CreateFile { parent: 0, name: 3 },
        },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 1 },
        },
        Event::Act {
            client: 1,
            action: CreateFile { parent: 0, name: 5 },
        },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 2 },
        },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Act {
            client: 2,
            action: CreateFolder {
                parent: 0,
                name: 13,
            },
        },
        Event::Plan { client: 2 },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 3 },
        },
        Event::Step { client: 2 },
        Event::Plan { client: 0 },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Step { client: 0 },
        Event::Act {
            client: 1,
            action: Delete { node: 0 },
        },
        Event::Act {
            client: 0,
            action: Delete { node: 15 },
        },
        Event::Act {
            client: 1,
            action: CreateFile { parent: 0, name: 7 },
        },
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 1 },
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn regression_user_move_into_clashing_folder() {
    use Action::*;
    let events = [
        Event::Act {
            client: 0,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Event::Act {
            client: 0,
            action: Move {
                node: 0,
                parent: 1,
                name: 9,
            },
        },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Act {
            client: 0,
            action: CreateFile {
                parent: 0,
                name: 14,
            },
        },
        Event::Act {
            client: 1,
            action: CreateFile {
                parent: 0,
                name: 15,
            },
        },
        Event::Plan { client: 1 },
        Event::Act {
            client: 0,
            action: Edit { file: 0 },
        },
        Event::Step { client: 1 },
        Event::Act {
            client: 0,
            action: Edit { file: 0 },
        },
        Event::Plan { client: 0 },
        Event::Plan { client: 0 },
        Event::Act {
            client: 0,
            action: Edit { file: 0 },
        },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Act {
            client: 1,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Event::Act {
            client: 0,
            action: Delete { node: 2 },
        },
        Event::Plan { client: 0 },
        Event::Act {
            client: 1,
            action: CreateFile { parent: 8, name: 0 },
        },
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Act {
            client: 1,
            action: Move {
                node: 1,
                parent: 9,
                name: 0,
            },
        },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
    ];
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn regression_create_under_concurrently_deleted_folder() {
    use Action::*;
    let events = [
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 2 },
        },
        Event::Plan { client: 0 },
        Event::Act {
            client: 1,
            action: CreateFolder { parent: 0, name: 6 },
        },
        Event::Act {
            client: 1,
            action: CreateFolder {
                parent: 14,
                name: 0,
            },
        },
        Event::Act {
            client: 1,
            action: CreateFolder { parent: 9, name: 3 },
        },
        Event::Step { client: 0 },
        Event::Act {
            client: 1,
            action: CreateFile {
                parent: 13,
                name: 0,
            },
        },
        Event::Act {
            client: 0,
            action: Delete { node: 0 },
        },
        Event::Act {
            client: 1,
            action: CreateFile { parent: 0, name: 1 },
        },
        Event::Act {
            client: 1,
            action: CreateFile { parent: 2, name: 0 },
        },
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Act {
            client: 0,
            action: Delete { node: 0 },
        },
        Event::Act {
            client: 1,
            action: Delete { node: 6 },
        },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Step { client: 0 },
        Event::Step { client: 1 },
    ];
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// A folder moved remotely into a place one client ignores, while that client holds a new
/// item in it (detached), then more moves around it.
#[test]
fn regression_detached_folder() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(0, CreateFolder { parent: 0, name: 0 }),
        Event::Plan { client: 0 },
        act(0, CreateFile { parent: 5, name: 0 }),
        act(0, CreateJunk { parent: 0 }),
        act(
            0,
            CreateFolder {
                parent: 1,
                name: 13,
            },
        ),
        act(0, CreateFile { parent: 1, name: 0 }),
        act(0, CreateFolder { parent: 3, name: 8 }),
        Event::Step { client: 0 },
        act(0, CreateJunk { parent: 0 }),
        Event::Plan { client: 1 },
        act(
            1,
            CreateFile {
                parent: 0,
                name: 13,
            },
        ),
        Event::Step { client: 1 },
        act(0, CreateFile { parent: 0, name: 5 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Step { client: 0 },
        act(1, CreateFolder { parent: 6, name: 0 }),
        Event::Step { client: 0 },
        act(0, CreateFolder { parent: 0, name: 3 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        act(
            0,
            Move {
                node: 0,
                parent: 11,
                name: 1,
            },
        ),
    ];
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Three clients, junk and a client that ignores `d` among concurrent creates, moves and
/// edits.
#[test]
fn regression_three_clients_with_filters() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(0, CreateFolder { parent: 0, name: 0 }),
        Event::Plan { client: 0 },
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
        act(0, CreateFile { parent: 6, name: 0 }),
        act(2, CreateFile { parent: 0, name: 6 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(
            2,
            CreateFile {
                parent: 0,
                name: 13,
            },
        ),
        act(0, Delete { node: 0 }),
        act(0, CreateFolder { parent: 0, name: 0 }),
        Event::Step { client: 1 },
        act(0, Edit { file: 0 }),
        act(1, CreateJunk { parent: 0 }),
        act(1, CreateFile { parent: 2, name: 0 }),
        act(
            0,
            Move {
                node: 0,
                parent: 1,
                name: 13,
            },
        ),
        act(0, CreateFile { parent: 0, name: 1 }),
        Event::Plan { client: 1 },
        act(0, CreateFile { parent: 0, name: 1 }),
        Event::Step { client: 1 },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        act(
            0,
            Move {
                node: 0,
                parent: 1,
                name: 2,
            },
        ),
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// An item parked under a temporary name whose original name a rule change then filters,
/// while another item this client ignores holds that name.
#[test]
fn regression_parked_under_a_name_that_became_ignored() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let file = |client| act(client, CreateFile { parent: 0, name: 0 });
    let events = [
        file(2),
        file(1),
        Event::Plan { client: 1 },
        act(2, ToggleIgnore),
        act(2, ToggleIgnore),
        act(1, CreateFolder { parent: 0, name: 5 }),
        act(
            2,
            CreateFolder {
                parent: 0,
                name: 13,
            },
        ),
        Event::Step { client: 1 },
        Event::Plan { client: 0 },
        Event::Plan { client: 0 },
        file(0),
        file(0),
        file(0),
        act(1, ToggleIgnore),
        act(
            2,
            Move {
                node: 2,
                parent: 2,
                name: 0,
            },
        ),
        file(0),
        file(0),
        file(0),
        Event::Plan { client: 2 },
        file(0),
        file(0),
        Event::Plan { client: 1 },
        Event::Step { client: 2 },
        Event::Step { client: 1 },
        act(1, Delete { node: 1 }),
        Event::Plan { client: 2 },
        act(2, ToggleIgnore),
        act(0, ToggleIgnore),
        Event::Step { client: 2 },
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn regression_fork_onto_a_name_that_stays_taken() {
    use Action::*;
    use Event::*;
    let events = [
        Act {
            client: 1,
            action: CreateFolder { parent: 9, name: 6 },
        },
        Act {
            client: 2,
            action: CreateFile {
                parent: 14,
                name: 10,
            },
        },
        Step { client: 0 },
        Step { client: 1 },
        Plan { client: 0 },
        Plan { client: 1 },
        Act {
            client: 1,
            action: CreateFolder { parent: 9, name: 7 },
        },
        Plan { client: 2 },
        Act {
            client: 1,
            action: Edit { file: 13 },
        },
        Step { client: 0 },
        Step { client: 0 },
        Step { client: 1 },
        Step { client: 2 },
        Plan { client: 2 },
        Step { client: 1 },
        Act {
            client: 2,
            action: Move {
                node: 3,
                parent: 12,
                name: 3,
            },
        },
        Plan { client: 0 },
        Step { client: 1 },
        Plan { client: 1 },
        Plan { client: 2 },
        Act {
            client: 0,
            action: CreateFile {
                parent: 13,
                name: 13,
            },
        },
        Plan { client: 0 },
        Step { client: 2 },
        Act {
            client: 2,
            action: Delete { node: 4 },
        },
        Act {
            client: 1,
            action: Move {
                node: 7,
                parent: 10,
                name: 3,
            },
        },
        Step { client: 0 },
        Step { client: 2 },
        Act {
            client: 2,
            action: CreateFile { parent: 0, name: 7 },
        },
        Step { client: 0 },
        Act {
            client: 1,
            action: ToggleIgnore,
        },
        Step { client: 0 },
        Act {
            client: 2,
            action: Edit { file: 4 },
        },
        Act {
            client: 1,
            action: Edit { file: 4 },
        },
        Plan { client: 2 },
        Step { client: 0 },
        Act {
            client: 1,
            action: Edit { file: 15 },
        },
        Step { client: 2 },
        Act {
            client: 0,
            action: Edit { file: 15 },
        },
        Step { client: 1 },
        Act {
            client: 0,
            action: CreateJunk { parent: 4 },
        },
        Act {
            client: 0,
            action: Edit { file: 11 },
        },
        Act {
            client: 0,
            action: Edit { file: 13 },
        },
        Step { client: 2 },
        Step { client: 2 },
        Step { client: 0 },
        Act {
            client: 0,
            action: Move {
                node: 5,
                parent: 12,
                name: 9,
            },
        },
        Act {
            client: 2,
            action: CreateFile {
                parent: 13,
                name: 2,
            },
        },
        Act {
            client: 2,
            action: CreateFile { parent: 6, name: 5 },
        },
        Act {
            client: 1,
            action: Move {
                node: 6,
                parent: 6,
                name: 7,
            },
        },
        Step { client: 0 },
        Step { client: 0 },
        Step { client: 0 },
        Plan { client: 2 },
        Plan { client: 0 },
        Step { client: 1 },
        Act {
            client: 2,
            action: CreateFile {
                parent: 15,
                name: 9,
            },
        },
        Plan { client: 2 },
        Act {
            client: 1,
            action: ToggleIgnore,
        },
        Act {
            client: 1,
            action: CreateFolder {
                parent: 12,
                name: 12,
            },
        },
        Step { client: 0 },
        Step { client: 2 },
        Act {
            client: 0,
            action: Move {
                node: 12,
                parent: 1,
                name: 5,
            },
        },
        Act {
            client: 2,
            action: CreateFile {
                parent: 10,
                name: 11,
            },
        },
        Plan { client: 1 },
        Plan { client: 2 },
        Act {
            client: 1,
            action: Move {
                node: 12,
                parent: 14,
                name: 1,
            },
        },
        Act {
            client: 1,
            action: ToggleIgnore,
        },
        Act {
            client: 0,
            action: Move {
                node: 11,
                parent: 4,
                name: 8,
            },
        },
        Act {
            client: 2,
            action: Edit { file: 14 },
        },
        Step { client: 2 },
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn regression_forget_of_a_cycle_of_bases() {
    use Action::*;
    use Event::*;
    let events = [
        Act {
            client: 2,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Act {
            client: 2,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Act {
            client: 2,
            action: CreateJunk { parent: 0 },
        },
        Act {
            client: 2,
            action: CreateFolder {
                parent: 10,
                name: 0,
            },
        },
        Plan { client: 2 },
        Step { client: 2 },
        Step { client: 2 },
        Step { client: 2 },
        Act {
            client: 2,
            action: CreateJunk { parent: 1 },
        },
        Act {
            client: 2,
            action: Move {
                node: 3,
                parent: 3,
                name: 1,
            },
        },
        Act {
            client: 2,
            action: Move {
                node: 0,
                parent: 6,
                name: 5,
            },
        },
        Act {
            client: 2,
            action: Move {
                node: 13,
                parent: 3,
                name: 13,
            },
        },
        Plan { client: 2 },
        Step { client: 2 },
        Act {
            client: 2,
            action: ToggleIgnore,
        },
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// A device confirms trashing a folder another device moved into a name it ignores. The drive
/// keeps the folder, so the confirmation must not be dropped as "nothing to delete any more"
/// before the trash ran: it would stay held for good.
#[test]
fn regression_confirmed_trash_of_a_folder_moved_into_an_ignored_name() {
    use Action::*;
    use Event::*;
    let act = |client, action| Act { client, action };
    let events = [
        act(1, ToggleIgnore),
        act(0, CreateFolder { parent: 0, name: 0 }),
        act(0, CreateFolder { parent: 0, name: 0 }),
        act(0, ToggleIgnore),
        act(0, CreateJunk { parent: 0 }),
        act(0, ToggleIgnore),
        act(0, ToggleIgnore),
        Plan { client: 0 },
        Step { client: 0 },
        act(0, CreateJunk { parent: 0 }),
        act(0, CreateFile { parent: 0, name: 1 }),
        Plan { client: 1 },
        act(0, CreateFile { parent: 4, name: 0 }),
        Step { client: 1 },
        act(
            1,
            Move {
                node: 0,
                parent: 1,
                name: 13,
            },
        ),
    ];
    if let Err(e) = run_with(2, Some(1), |sim| sim, &events) {
        panic!("{e}");
    }
}

/// One client edits a file and deletes it; the other edits the version before. The edit
/// wins, but over a version Recently deleted holds that the editor never had: that one stays
/// there, and the edit becomes a new item.
#[test]
fn regression_edit_over_a_version_deleted_elsewhere() {
    use Action::*;
    let (c0, c1) = (|| 0, || 1);
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(c1(), CreateFile { parent: 0, name: 0 }),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        act(c0(), Edit { file: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        act(c0(), Delete { node: 0 }),
        act(c1(), Edit { file: 0 }),
    ];
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// The file is deleted on the drive between a download's fetch and its commit: the commit
/// settles against the version it fetched, so the delete reaches the client. Settled against
/// the drive (nothing there), the file had no base, and the client put it back.
#[test]
fn regression_a_delete_during_a_download_is_taken() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let mut sim = Sim::new(2).with_split_downloads();
    sim.run(&[
        act(0, CreateFile { parent: 0, name: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
    ]);
    let file = sim.item(0, "a").expect("the file");
    sim.run(&[
        Event::Plan { client: 1 },
        Event::Step { client: 1 }, // fetched
        act(0, Delete { node: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 }, // removed on the drive
        Event::Step { client: 1 }, // in place, committed
    ]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    assert!(sim.is_removed(file), "the file came back on the drive");
    assert_eq!(sim.item(1, "a"), None);
}

/// The drive renames a file and tags it red (client 0): client 1's plan moves it and tags it,
/// two writes on the node, the second checking the base at the version the first settled it to.
/// Then client 1's user removes red: it stays removed. (Checked against the plan's version, the
/// second write's base was left alone, red unagreed, and the next plan wrote red back.)
#[test]
fn regression_a_tag_removed_after_a_rename_and_tag_stays_removed() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let mut sim = Sim::new(2);
    sim.run(&[act(0, CreateFile { parent: 0, name: 0 })]);
    assert!(sim.quiesce(10));
    let file = sim.item(0, "a").expect("the file");
    let root = sim.pick(0, NodeId::ROOT, true).expect("the root");
    let it = sim.pick(0, file, false).expect("on client 0");
    sim.run(&[
        act(
            0,
            Move {
                node: it,
                parent: root,
                name: 1,
            },
        ), // b
        act(0, Tag { node: it, tag: 0 }), // Red
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 1 },
    ]);
    assert_eq!(sim.local_tags(1, "b"), Some(vec!["Red:0".to_owned()]));
    let it = sim.pick(1, file, false).expect("on client 1");
    sim.run(&[act(1, Tag { node: it, tag: 0 })]); // Red removed
    assert!(sim.quiesce(10));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(sim.server_tags("b"), Some(Vec::new()));
}

/// A download put in place, its commit lost, and the next sync's scan doesn't hash the file yet
/// (the first crash leaves it so): recovery keeps the record and the plan defers the node; the
/// sync after settles it. A record kept holds its node off placements, never for good.
#[test]
fn a_record_kept_past_recovery_is_settled_later() {
    use Action::*;
    let mut sim = Sim::new(2).with_split_downloads();
    sim.run(&[
        Event::Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
        Event::Step { client: 1 },  // fetched
        Event::Crash { client: 1 }, // put in place, its commit lost
        Event::Plan { client: 1 },  // the record kept
    ]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}

/// Client 1's plan downloads both files a changed; the user edits b there after the plan, and
/// the download of a fails its commit once in place. Recovery settles a by its own item alone:
/// b's download still finds the edit, which isn't lost (the engine's recovery rescanned the
/// folder, and b's next guard passed on the edit).
#[test]
fn a_sibling_edit_survives_a_failed_commit() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let mut sim = Sim::new(2);
    sim.run(&[
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 1 }),
    ]);
    assert!(sim.quiesce(10));
    let (a, b) = (sim.item(0, "a").expect("a"), sim.item(0, "b").expect("b"));
    let (a0, b0) = (
        sim.file_pick(0, a).expect("a on 0"),
        sim.file_pick(0, b).expect("b on 0"),
    );
    sim.run(&[
        act(0, Edit { file: a0 }),
        act(0, Edit { file: b0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
    ]);
    let b1 = sim.file_pick(1, b).expect("b on 1");
    sim.run(&[act(1, Edit { file: b1 })]);
    sim.fail_next_commit();
    sim.run(&[Event::Step { client: 1 }, Event::Step { client: 1 }]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}

/// A tagged folder is deleted here while another device adds to it: the delete loses, and the
/// folder is made again, with its record (synced before). The client dies once it is made,
/// before its tags are written: the record claims none, so recovery takes only what the folder
/// and the drive both have, and the next plan writes the drive's tags on the folder. A record
/// that claimed them up front took the bare folder for agreed: the next plan removed them from
/// the drive.
#[test]
fn regression_a_folder_made_again_keeps_its_tags() {
    use Action::*;
    let (c0, c1) = (|| 0, || 1);
    let act = |client, action| Event::Act { client, action };
    let mut sim = Sim::new(2);
    sim.run(&[
        act(c0(), CreateFolder { parent: 0, name: 0 }),
        act(c0(), Tag { node: 0, tag: 0 }), // Red
    ]);
    assert!(sim.quiesce(10));
    let folder = sim.item(0, "a").expect("the folder");
    let into = sim.pick(1, folder, true).expect("on client 1");
    let it = sim.pick(0, folder, false).expect("on client 0");
    sim.run(&[
        act(
            c1(),
            CreateFile {
                parent: into,
                name: 1,
            },
        ),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        act(c0(), Delete { node: it }),
        Event::Plan { client: 0 },
        Event::Crash { client: 0 },
    ]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(sim.server_tags("a"), Some(vec!["Red:6".to_owned()]));
}

/// Found by a 150,000-case run: client 1 starts ignoring `d` between planning its download and
/// running it, and stops again before its next plan. A rule change counts from the next run
/// (the executor holds one filter for its run): the download settles, and a tag removed on the
/// drive later stays removed. Settled with the new rule, `d` had no base, and its next plan
/// merged the tags as if both sides had made the file.
#[test]
fn regression_a_rule_change_counts_from_the_next_plan() {
    use Action::*;
    let (c0, c1) = (|| 0, || 1);
    let act = |client, action| Event::Act { client, action };
    let events = [
        act(c1(), ToggleIgnore), // client 1 syncs `d`
        act(c0(), CreateFile { parent: 0, name: 5 }),
        act(c0(), Tag { node: 0, tag: 1 }), // Work
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Plan { client: 1 },
        act(c1(), ToggleIgnore),
        Event::Step { client: 1 },
        act(c1(), ToggleIgnore),
        act(c0(), Tag { node: 0, tag: 1 }), // Work removed
        act(c0(), Edit { file: 0 }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
    ];
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Deleted on one client, restored (onto a name taken meanwhile), then purged: the clients
/// converge at each step.
#[test]
fn restore_and_purge_converge() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let sync = |client| {
        [
            Event::Plan { client },
            Event::Step { client },
            Event::Step { client },
        ]
    };
    let mut events = vec![act(0, CreateFile { parent: 0, name: 0 })];
    events.extend(sync(0));
    events.extend(sync(1));
    events.push(act(1, Delete { node: 0 }));
    events.extend(sync(1));
    events.push(act(0, CreateFile { parent: 0, name: 0 })); // the name, taken again
    events.extend(sync(0));
    events.push(act(0, Restore { node: 0 }));
    events.extend(sync(0));
    events.extend(sync(1));
    events.push(act(1, Delete { node: 0 }));
    events.extend(sync(1));
    // Daily purges, until what was deleted is old enough.
    events.extend([act(1, Purge), act(1, Purge), act(1, Purge)]);
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Emptying Recently deleted as listed while other devices work: after the listing a listed
/// folder comes back, and (or not) the phone saves an edit of a file a listed folder's delete
/// took along (its own removal still on its way). What was listed goes (or, purged, all of
/// Recently deleted), but never a live node: the folder holding the file stays, and comes back
/// with the edit; nothing written is lost, and the clients converge.
#[test]
fn emptying_as_listed_spares_what_came_after_the_listing() {
    use Action::*;
    use hbsync_types::NodeId;
    let (a, b, c_txt) = (0, 1, 3); // NAMES: "a", "b", "c.txt"
    let act = |client, action| Event::Act { client, action };
    let folder = |sim: &Sim, path: &str| -> usize {
        let node = match path {
            "" => NodeId::ROOT,
            _ => sim.item(0, path).expect(path),
        };
        sim.pick(0, node, true).expect(path)
    };
    let create = |sim: &mut Sim, parent: &str, name, file: bool| {
        let parent = folder(sim, parent);
        let action = match file {
            true => CreateFile { parent, name },
            false => CreateFolder { parent, name },
        };
        sim.run(&[act(0, action)]);
    };
    let mut sync0 = vec![Event::Plan { client: 0 }];
    sync0.extend([Event::Step { client: 0 }; 12]);
    for (edited, purge) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut sim = Sim::new(2);
        create(&mut sim, "", a, false);
        create(&mut sim, "", b, false);
        create(&mut sim, "a", c_txt, true);
        create(&mut sim, "b", c_txt, true);
        assert!(sim.quiesce(30), "no convergence");
        let (node_a, node_b) = (sim.item(0, "a").expect("a"), sim.item(0, "b").expect("b"));
        // Client 0 deletes `b`, and `a` as far as the folder (the file in it, still live, was
        // taken along); then lists Recently deleted.
        let node = sim.pick(0, node_b, false).expect("b");
        sim.run(&[act(0, Delete { node })]);
        sim.run(&sync0);
        let node = sim.pick(0, node_a, false).expect("a");
        sim.run(&[act(0, Delete { node })]);
        sim.run(&[Event::Plan { client: 0 }, Event::Step { client: 0 }]);
        assert!(sim.is_removed(node_a), "the folder went first");
        sim.run(&[act(0, ListDeleted)]);
        // Since the listing: `b` comes back, and the phone may edit the file in `a`. (No device
        // syncs here: its sync would mark the file removed, a removal recorded after the
        // listing, which keeps `a`.)
        let node = sim.removed_pick(node_b).expect("b removed");
        sim.run(&[act(0, Restore { node })]);
        if edited {
            sim.run(&[act(0, EditTaken { pick: 0 })]);
        }
        sim.run(&[act(0, if purge { Purge } else { EmptyListed })]);
        assert!(sim.is_removed(node_a), "`a` holds a live node: it stays");
        assert!(sim.quiesce(30), "no convergence");
        if let Err(e) = sim.check() {
            panic!("edited {edited}, purge {purge}: {e}");
        }
        let mut expected = vec!["b", "b/c.txt"];
        if edited {
            expected.splice(0..0, ["a", "a/c.txt"]);
        }
        assert_eq!(
            sim.server_paths(),
            expected,
            "edited {edited}, purge {purge}"
        );
    }
}

/// Found by a 150,000-case run (2026-10-16): two devices hold the same file without a base,
/// both edited against the drive, and fork it. The copy id was the same for both (`fork_id`:
/// same node, same bytes, no base version), and the first device's copy was on the drive
/// (detached) when the second planned its fork: the copy's merged state was the drive's node,
/// not the fork's, so the fork took no name and the copy was created on the drive as "". Each
/// computer now makes its own copy, under its own id.
#[test]
fn regression_two_devices_fork_their_own_copies() {
    use hbsync_sim::Action::*;
    use hbsync_sim::Event::*;
    let events = [
        Act {
            client: 2,
            action: CreateFolder { parent: 0, name: 1 },
        },
        Act {
            client: 2,
            action: CreateFile { parent: 2, name: 0 },
        },
        Act {
            client: 1,
            action: CreateFile {
                parent: 0,
                name: 11,
            },
        },
        Plan { client: 2 },
        Step { client: 2 },
        Step { client: 2 },
        Plan { client: 0 },
        Act {
            client: 2,
            action: Move {
                node: 1,
                parent: 1,
                name: 8,
            },
        },
        Act {
            client: 1,
            action: CreateFile { parent: 0, name: 9 },
        },
        Plan { client: 1 },
        Plan { client: 2 },
        Step { client: 2 },
        Step { client: 1 },
        Act {
            client: 2,
            action: CreateFile { parent: 1, name: 5 },
        },
        Step { client: 1 },
        Act {
            client: 1,
            action: CreateFile { parent: 1, name: 0 },
        },
        Step { client: 1 },
        Act {
            client: 2,
            action: Delete { node: 15 },
        },
        Step { client: 1 },
        Step { client: 1 },
        Act {
            client: 2,
            action: Edit { file: 8 },
        },
        Step { client: 0 },
        Plan { client: 2 },
        Step { client: 2 },
        Step { client: 0 },
        Step { client: 2 },
        Plan { client: 1 },
        Step { client: 1 },
        Step { client: 2 },
        Step { client: 2 },
        Step { client: 1 },
        Plan { client: 1 },
        Plan { client: 2 },
        Step { client: 2 },
        Step { client: 1 },
        Step { client: 2 },
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Found by a 150,000-case run (2026-10-16), failing on main: a tag added on client 1 was lost
/// ("lost tag \"Red\"") with clients that sync only when asked and the guard at 1. Passes
/// with sim-role-fixes' tag fixes.
#[test]
fn regression_a_tag_lost_when_syncing_only_when_asked() {
    use hbsync_sim::Action::*;
    use hbsync_sim::Event::*;
    let events = [
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Act {
            client: 2,
            action: CreateFolder { parent: 0, name: 8 },
        },
        Act {
            client: 2,
            action: CreateFile { parent: 6, name: 6 },
        },
        Act {
            client: 0,
            action: CreateFolder { parent: 0, name: 3 },
        },
        Plan { client: 0 },
        Act {
            client: 1,
            action: CreateFolder { parent: 0, name: 6 },
        },
        Plan { client: 2 },
        Step { client: 0 },
        Step { client: 2 },
        Plan { client: 1 },
        Step { client: 0 },
        Step { client: 2 },
        Plan { client: 1 },
        Step { client: 1 },
        Act {
            client: 1,
            action: Edit { file: 0 },
        },
        Step { client: 1 },
        Step { client: 1 },
        Act {
            client: 0,
            action: Delete { node: 5 },
        },
        Step { client: 1 },
        Act {
            client: 0,
            action: CreateJunk { parent: 0 },
        },
        Step { client: 1 },
        Step { client: 1 },
        Plan { client: 0 },
        Act {
            client: 1,
            action: Move {
                node: 12,
                parent: 1,
                name: 0,
            },
        },
        Step { client: 0 },
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 8 },
        },
        Act {
            client: 1,
            action: Tag { node: 2, tag: 4 },
        },
        Step { client: 0 },
        Plan { client: 1 },
        Step { client: 1 },
        Step { client: 0 },
        Step { client: 0 },
    ];
    run_settled(
        3,
        Some(1),
        |sim| sim,
        |sim| sim.quiesce_announced(300),
        &events,
    )
    .map_err(|e| e.to_string())
    .unwrap_or_else(|e| panic!("{e}"));
}

/// A folder moved into another after that one's removal (by a device that didn't know of the
/// delete yet) is a change of its own: the purge takes nothing in it along, however old, and
/// it keeps the folder it went into.
#[test]
fn regression_a_folder_moved_into_a_removed_one_keeps_what_is_in_it() {
    use Action::*;
    use hbsync_types::NodeId;
    let mut sim = Sim::new(2);
    let pick = |sim: &Sim, c: usize, path: &str, as_parent: bool| -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(c, path).expect(path)
        };
        sim.pick(c, node, as_parent).expect(path)
    };
    let act = |client, action| Event::Act { client, action };
    // NAMES: "a", "b", "c.txt" (client 1 ignores "d").
    for name in [0, 1] {
        let parent = pick(&sim, 0, "", true);
        sim.run(&[act(0, CreateFolder { parent, name })]);
    }
    let parent = pick(&sim, 0, "b", true);
    sim.run(&[act(0, CreateFile { parent, name: 3 })]);
    assert!(sim.quiesce(30), "no convergence");
    // Client 1 moves b into a, planned before client 0's delete of a lands.
    let (node, parent) = (pick(&sim, 1, "b", false), pick(&sim, 1, "a", true));
    sim.run(&[
        act(
            1,
            Move {
                node,
                parent,
                name: 1,
            },
        ),
        Event::Plan { client: 1 },
    ]);
    let node = pick(&sim, 0, "a", false);
    sim.run(&[
        act(0, Delete { node }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        Event::Step { client: 1 },
        // Daily purges, until a's removal is old enough.
        act(0, Purge),
        act(0, Purge),
        act(0, Purge),
        act(0, Purge),
    ]);
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert!(
        sim.server_paths().iter().any(|p| p == "a/b/c.txt"),
        "b and c.txt stay, in a: {:?}",
        sim.server_paths()
    );
}

/// Purges after a folder's delete stopped half way (the folder removed, the file in it not
/// marked yet): the purge, once the folder is kept long enough, removes the file (taken along)
/// and never deletes it for good at once; the delete carried on finds it removed already, and
/// the client that saw it and the one away since before it converge, nothing lost or back.
#[test]
fn regression_a_purge_mid_delete_leaves_what_the_folder_took_along() {
    use Action::*;
    use hbsync_types::NodeId;
    let mut sim = Sim::new(2);
    let folder = |sim: &Sim, path: &str| -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(0, path).expect(path)
        };
        sim.pick(0, node, true).expect(path)
    };
    let act = |client, action| Event::Act { client, action };
    let (d, a) = (5, 0); // NAMES: "d", "a"
    let parent = folder(&sim, "");
    sim.run(&[act(0, CreateFolder { parent, name: d })]);
    let parent = folder(&sim, "d");
    sim.run(&[act(0, CreateFile { parent, name: a })]);
    assert!(sim.quiesce(30), "no convergence");
    let node = sim.pick(0, sim.item(0, "d").expect("d"), false).expect("d");
    // The folder goes first; the purge comes before the file's own removal, and client 1, away
    // meanwhile, syncs before client 0 goes on.
    sim.run(&[
        act(0, Delete { node }),
        Event::Plan { client: 0 },
        Event::Step { client: 0 },
        // Daily purges, until the folder's removal is old enough.
        act(0, Purge),
        act(0, Purge),
        act(0, Purge),
        Event::Plan { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 1 },
        Event::Step { client: 1 },
    ]);
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(
        sim.server_paths(),
        Vec::<String>::new(),
        "nothing came back"
    );
}

/// Client 0 and the other devices, with folder "b" holding "b/c.txt" and "b/a/c.txt", all
/// synced; and the ids of b, b/a, b/c.txt and b/a/c.txt.
fn stopped_delete_setup() -> (Sim, [NodeId; 4]) {
    use Action::*;
    let mut sim = Sim::new(2);
    let folder = |sim: &Sim, path: &str| -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(0, path).expect(path)
        };
        sim.pick(0, node, true).expect(path)
    };
    let act = |client, action| Event::Act { client, action };
    let (a, b, c) = (0, 1, 3); // NAMES: "a", "b", "c.txt"
    let parent = folder(&sim, "");
    sim.run(&[act(0, CreateFolder { parent, name: b })]);
    let parent = folder(&sim, "b");
    sim.run(&[
        act(0, CreateFolder { parent, name: a }),
        act(0, CreateFile { parent, name: c }),
    ]);
    let parent = folder(&sim, "b/a");
    sim.run(&[act(0, CreateFile { parent, name: c })]);
    assert!(sim.quiesce(30), "no convergence");
    let ids = ["b", "b/a", "b/c.txt", "b/a/c.txt"].map(|p| sim.item(0, p).expect(p));
    (sim, ids)
}

/// Another device deletes folder b and stops right after the folder, never to come back; no
/// device syncs meanwhile. Once b is kept long enough, the purge removes what it took along, a
/// folder before what is in it, to Recently deleted; once their own removal is kept as long,
/// all of it goes for good. The devices converge, nothing lost.
#[test]
fn regression_the_purge_finishes_a_delete_stopped_half_way() {
    use Action::*;
    let (mut sim, [b, a, c, ac]) = stopped_delete_setup();
    let pick = sim.stopped_pick(b).expect("b");
    let act = |action| Event::Act { client: 0, action };
    sim.run(&[act(DeleteStopped { pick, done: 0 })]);
    assert!(sim.is_removed(b));
    assert!(
        ![a, c, ac].iter().any(|n| sim.is_removed(*n)),
        "taken along, live"
    );
    // Daily purges, until b's removal is old enough.
    sim.run(&[act(Purge), act(Purge), act(Purge)]);
    assert!(
        [b, a, c, ac].iter().all(|n| sim.is_removed(*n)),
        "removed now"
    );
    sim.run(&[act(Purge), act(Purge), act(Purge)]);
    assert!(!sim.is_removed(b), "gone for good");
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(sim.server_paths(), Vec::<String>::new());
}

/// As [`regression_the_purge_finishes_a_delete_stopped_half_way`], but the phone edits b/a/c.txt
/// after the delete: written after the removal, it isn't taken along. The purge leaves it live,
/// and b/a above it, and b holding them; it removes b/c.txt only. The devices bring b back
/// with them, nothing lost.
#[test]
fn regression_the_purge_leaves_what_was_written_after_a_stopped_delete() {
    use Action::*;
    let (mut sim, [b, a, c, ac]) = stopped_delete_setup();
    let pick = sim.stopped_pick(b).expect("b");
    let act = |action| Event::Act { client: 0, action };
    sim.run(&[act(DeleteStopped { pick, done: 0 })]);
    // EditTaken picks among the files taken along, in id order.
    let pick = usize::from(ac > c);
    sim.run(&[act(EditTaken { pick })]);
    sim.run(&[act(Purge), act(Purge), act(Purge)]);
    assert!(sim.is_removed(c), "taken along: removed");
    assert!(
        !sim.is_removed(a) && !sim.is_removed(ac),
        "written after: live"
    );
    sim.run(&[act(Purge), act(Purge), act(Purge)]);
    assert!(sim.is_removed(b), "b holds them");
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(sim.server_paths(), ["b", "b/a", "b/a/c.txt"]);
}

/// A file put into a folder after another device removed it (with what was in it) isn't in
/// that delete: the purge leaves the folder while the file is live in it, so the file keeps its
/// folder, which comes back, and nothing is lost.
#[test]
fn regression_a_file_written_into_a_removed_folder_holds_it_through_the_purge() {
    use Action::*;
    use hbsync_types::NodeId;
    let mut sim = Sim::new(2);
    let folder = |sim: &Sim, c: usize, path: &str| -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(c, path).expect(path)
        };
        sim.pick(c, node, true).expect(path)
    };
    let act = |client, action| Event::Act { client, action };
    // NAMES: "a", "c.txt", "b" (client 1 ignores "d").
    let (folder_name, c, b) = (0, 3, 1);
    let parent = folder(&sim, 0, "");
    sim.run(&[act(
        0,
        CreateFolder {
            parent,
            name: folder_name,
        },
    )]);
    let parent = folder(&sim, 0, "a");
    sim.run(&[act(0, CreateFile { parent, name: c })]);
    assert!(sim.quiesce(30), "no convergence");
    // Client 1 plans its new file before client 0's delete of the folder lands; the upload
    // goes into the removed folder.
    let parent = folder(&sim, 1, "a");
    sim.run(&[
        act(1, CreateFile { parent, name: b }),
        Event::Plan { client: 1 },
    ]);
    let node = sim.pick(0, sim.item(0, "a").expect("a"), false).expect("a");
    sim.run(&[
        act(0, Delete { node }),
        Event::Plan { client: 0 },
        // The folder, then the file in it.
        Event::Step { client: 0 },
        Event::Step { client: 0 },
        Event::Step { client: 1 },
    ]);
    sim.run(&[
        // Daily purges, until the folder's removal is old enough.
        act(0, Purge),
        act(0, Purge),
        act(0, Purge),
    ]);
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    // The file brings its folder back (not left at the top, the folder gone for good).
    assert_eq!(sim.server_paths(), ["a", "a/b"]);
}

/// Tags meet on every device: a Mac's tag and colour change, a Linux tag (Linux keeps no
/// colours, and keeps the Mac's), and a Windows device, which has no tags, keeps them through
/// its own edit and rename. A tag removed anywhere goes everywhere.
#[test]
fn tags_meet_across_macos_linux_and_windows() {
    use Action::*;
    let mut sim = Sim::new(3); // c0 macOS, c1 Linux, c2 Windows
    let act = |client, action| Event::Act { client, action };
    sim.run(&[act(0, CreateFile { parent: 0, name: 0 })]);
    assert!(sim.quiesce(10));
    sim.run(&[
        act(0, Tag { node: 0, tag: 0 }), // Red, on the Mac
        act(1, Tag { node: 0, tag: 1 }), // Work, on Linux at the same time
    ]);
    assert!(sim.quiesce(10));
    assert_eq!(
        sim.server_tags("a"),
        Some(vec!["Red:6".into(), "Work:0".into()])
    );
    sim.run(&[
        act(0, Colour { node: 0, tag: 0 }),
        act(2, Edit { file: 0 }),
        act(
            2,
            Move {
                node: 0,
                parent: 0,
                name: 1,
            },
        ),
    ]);
    assert!(sim.quiesce(10));
    assert_eq!(
        sim.server_tags("b"),
        Some(vec!["Red:7".into(), "Work:0".into()])
    );
    sim.run(&[act(1, Tag { node: 0, tag: 0 })]); // Linux removes Red
    assert!(sim.quiesce(10));
    assert_eq!(sim.server_tags("b"), Some(vec!["Work:0".into()]));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}

/// A tag change is not an edit: against a delete elsewhere the delete wins (the item is in
/// Recently deleted), and against an edit or a move both go through.
#[test]
fn a_tag_change_loses_to_a_delete_and_meets_an_edit_or_move() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let sync = |client| {
        [
            Event::Plan { client },
            Event::Step { client },
            Event::Step { client },
        ]
    };
    let mut sim = Sim::new(2);
    sim.run(&[
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 1 }),
    ]);
    assert!(sim.quiesce(10));
    // `a`: tagged here, deleted there first. `b`: tagged here, edited and renamed there.
    sim.run(&[
        act(0, Tag { node: 0, tag: 0 }),
        act(0, Tag { node: 1, tag: 1 }),
        act(1, Delete { node: 0 }),
        act(1, Edit { file: 1 }),
        act(
            1,
            Move {
                node: 1,
                parent: 0,
                name: 3,
            },
        ),
    ]);
    sim.run(&sync(1));
    assert!(sim.quiesce(10));
    assert_eq!(sim.server_tags("a"), None);
    assert_eq!(sim.server_tags("c.txt"), Some(vec!["Work:0".into()]));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}

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

/// c0 detaches its edited copy of the held role folder; the phone moves the file to the top,
/// c0 downloads it there, then ignores its new name: detached again, it got the id of its first
/// detached copy, and the download overwrote the edit.
#[test]
fn regression_detached_again_while_the_first_copy_lives() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, Delete, Edit, Move, Phone, ToggleIgnore};
    let act = |client, action| Event::Act { client, action };
    let (plan, step) = (
        |client| Event::Plan { client },
        |client| Event::Step { client },
    );
    let events = [
        act(2, CreateFolder { parent: 0, name: 0 }),
        act(2, ToggleIgnore),
        act(2, CreateFile { parent: 2, name: 0 }),
        act(0, Phone { op: 4, pick: 5 }),
        act(2, Delete { node: 0 }),
        act(2, ToggleIgnore),
        act(2, ToggleIgnore),
        act(0, Edit { file: 0 }),
        act(0, Phone { op: 0, pick: 0 }),
        plan(0),
        act(2, ToggleIgnore),
        act(0, ToggleIgnore),
        act(0, Phone { op: 11, pick: 8 }),
        act(1, CreateFile { parent: 0, name: 0 }),
        plan(2),
        step(2),
        act(
            2,
            Move {
                node: 0,
                parent: 0,
                name: 13,
            },
        ),
        act(2, CreateFile { parent: 0, name: 0 }),
        step(0),
        plan(2),
        step(0),
        step(2),
        plan(0),
        step(0),
        step(0),
        step(2),
        step(0),
        step(0),
        step(0),
    ];
    if let Err(e) = run_with(3, None, Sim::with_role_folder_held, &events) {
        panic!("{e}");
    }
}

/// c2 starts ignoring `d`, which it synced, after c0 tagged it: `d` was taken off the drive as
/// unchanged since c2 synced it (only place and bytes were compared). It stays, with its tag.
#[test]
fn regression_a_folder_tagged_elsewhere_doesnt_leave() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, Purge, Tag, ToggleIgnore};
    let act = |client, action| Event::Act { client, action };
    let (plan, step) = (
        |client| Event::Plan { client },
        |client| Event::Step { client },
    );
    let events = [
        act(0, CreateFolder { parent: 0, name: 5 }),
        plan(0),
        step(0),
        act(1, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 7, name: 0 }),
        act(0, Tag { node: 2, tag: 0 }),
        act(1, ToggleIgnore),
        plan(2),
        plan(1),
        plan(0),
        step(1),
        step(2),
        act(2, ToggleIgnore),
        step(0),
        act(0, CreateFile { parent: 0, name: 0 }),
        step(0),
        plan(2),
        step(2),
        step(2),
        act(1, CreateFile { parent: 6, name: 0 }),
        act(0, Purge),
        act(0, Purge),
        act(0, Purge),
        plan(1),
        step(1),
    ];
    for setup in [(|sim| sim) as fn(Sim) -> Sim, Sim::with_role_folder] {
        let mut sim = setup(Sim::new(3));
        sim.run(&events);
        assert!(sim.quiesce(30), "no convergence");
        if let Err(e) = sim.check() {
            panic!("{e}");
        }
        assert_eq!(sim.server_tags("d"), Some(vec!["Red:6".to_owned()]));
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

/// A folder moved into another here while that one was moved into it elsewhere, its old parent
/// deleted here: undoing the local move puts it back into the deleted folder, which then comes
/// back for it (§6.3) instead of being deleted on the drive under a live child, a plan no
/// executor can carry out (found by `main_folders_leave_role_folders_alone`).
#[test]
fn regression_an_undone_move_brings_back_the_folder_deleted_here() {
    use hbsync_sim::Action::{CreateFile, CreateFolder, CreateJunk, Delete, Move, ToggleIgnore};
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(0, CreateFile { parent: 0, name: 2 }),
        act(1, CreateFile { parent: 0, name: 0 }),
        act(2, CreateFolder { parent: 0, name: 2 }),
        act(2, ToggleIgnore),
        act(2, ToggleIgnore),
        act(1, CreateFile { parent: 0, name: 0 }),
        act(0, ToggleIgnore),
        act(2, CreateFolder { parent: 0, name: 0 }),
        act(0, CreateFolder { parent: 0, name: 6 }),
        act(0, CreateFile { parent: 0, name: 8 }),
        act(
            0,
            CreateFolder {
                parent: 14,
                name: 1,
            },
        ),
        act(2, CreateFolder { parent: 1, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateJunk { parent: 0 }),
        plan(0),
        step(0),
        plan(2),
        act(0, CreateJunk { parent: 0 }),
        step(2),
        step(2),
        act(2, CreateFolder { parent: 2, name: 0 }),
        step(2),
        step(0),
        step(2),
        act(
            2,
            Move {
                node: 13,
                parent: 2,
                name: 1,
            },
        ),
        plan(0),
        act(0, CreateFile { parent: 0, name: 0 }),
        step(0),
        act(0, CreateFile { parent: 1, name: 0 }),
        act(2, Delete { node: 0 }),
        act(0, CreateFile { parent: 1, name: 0 }),
        step(0),
        step(0),
        act(
            0,
            Move {
                node: 3,
                parent: 4,
                name: 6,
            },
        ),
        act(0, CreateFile { parent: 2, name: 0 }),
    ];
    if let Err(e) = run_with(3, None, Sim::with_role_folder, &events) {
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

/// A device's own folder removal has its time from the write's answer (odin-core #1971): the
/// plan made right after it, before any feed read, sees that time, so what is in the folder
/// went with it, and a restore of the folder brings it all back.
#[test]
fn regression_an_own_removal_time_takes_along_what_is_in_the_folder() {
    use Action::*;
    use hbsync_types::NodeId;
    let mut sim = Sim::new(2);
    let run = |sim: &mut Sim, events: &[Event]| sim.run(events);
    let folder = |sim: &Sim, path: &str| -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(0, path).expect(path)
        };
        sim.pick(0, node, true).expect(path)
    };
    let (d, a, b) = (5, 0, 1); // NAMES: "d", "a", "b"
    let parent = folder(&sim, "");
    run(
        &mut sim,
        &[Event::Act {
            client: 0,
            action: CreateFolder { parent, name: d },
        }],
    );
    let parent = folder(&sim, "d");
    for name in [a, b] {
        run(
            &mut sim,
            &[Event::Act {
                client: 0,
                action: CreateFile { parent, name },
            }],
        );
    }
    assert!(sim.quiesce(30), "no convergence");
    let photos = sim.item(0, "d").expect("d");
    let node = sim.pick(0, photos, false).expect("d");
    // The folder goes first; the next plan comes before any feed read.
    run(
        &mut sim,
        &[
            Event::Act {
                client: 0,
                action: Delete { node },
            },
            Event::Plan { client: 0 },
            Event::Step { client: 0 },
            Event::Plan { client: 0 },
        ],
    );
    assert!(sim.quiesce(30), "no convergence");
    if let Err(e) = sim.check() {
        panic!("{e}");
    }
    assert_eq!(sim.server_paths(), Vec::<String>::new());
    // All of it is deleted with the folder: restoring it brings it all back.
    let node = sim.removed_pick(photos).expect("d removed");
    run(
        &mut sim,
        &[Event::Act {
            client: 1,
            action: Restore { node },
        }],
    );
    assert!(sim.quiesce(30), "no convergence");
    assert_eq!(sim.server_paths(), ["d", "d/a", "d/b"]);
}

/// The Photos example: on Monday `Photos/a.jpg` is deleted on its own; on Tuesday the folder
/// `Photos`, still holding `b.jpg` and a subfolder with a file. Restoring `Photos` brings back
/// `b.jpg` and the subfolder's file, not `a.jpg`: that stays in Recently deleted, and comes back
/// on its own, into `Photos`. (In the simulator's names: `Photos` is `d`, `a.jpg` `a`, `b.jpg`
/// `b`, the subfolder `d/d` and its file `d/d/c.txt`.)
#[test]
fn regression_photos_restored_without_what_was_deleted_from_it_before() {
    use Action::*;
    use hbsync_types::NodeId;
    let mut sim = Sim::new(2);
    let act = |sim: &mut Sim, client, action| sim.run(&[Event::Act { client, action }]);
    let settle = |sim: &mut Sim| assert!(sim.quiesce(30), "no convergence");
    let folder = |sim: &Sim, path: &str| -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(0, path).expect(path)
        };
        sim.pick(0, node, true).expect(path)
    };
    let (d, a, b, c_txt) = (5, 0, 1, 3); // NAMES: "d", "a", "b", "c.txt"
    let parent = folder(&sim, "");
    act(&mut sim, 0, CreateFolder { parent, name: d });
    let parent = folder(&sim, "d");
    act(&mut sim, 0, CreateFile { parent, name: a });
    act(&mut sim, 0, CreateFile { parent, name: b });
    act(&mut sim, 0, CreateFolder { parent, name: d });
    let parent = folder(&sim, "d/d");
    act(
        &mut sim,
        0,
        CreateFile {
            parent,
            name: c_txt,
        },
    );
    settle(&mut sim);
    let (photos, a_jpg) = (
        sim.item(0, "d").expect("d"),
        sim.item(0, "d/a").expect("d/a"),
    );

    // Monday: a.jpg on its own. Tuesday: the folder.
    let node = sim.pick(0, a_jpg, false).expect("a");
    act(&mut sim, 0, Delete { node });
    settle(&mut sim);
    let node = sim.pick(0, photos, false).expect("d");
    act(&mut sim, 0, Delete { node });
    settle(&mut sim);
    assert_eq!(sim.server_paths(), Vec::<String>::new());

    // The other device restores the folder: b.jpg and the subfolder come back, a.jpg doesn't.
    let node = sim.removed_pick(photos).expect("d removed");
    act(&mut sim, 1, Restore { node });
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["d", "d/b", "d/d", "d/d/c.txt"]);
    assert!(sim.is_removed(a_jpg), "a.jpg stays in Recently deleted");

    // a.jpg on its own, into Photos.
    let node = sim.removed_pick(a_jpg).expect("a removed");
    act(&mut sim, 1, Restore { node });
    settle(&mut sim);
    assert_eq!(sim.server_paths(), ["d", "d/a", "d/b", "d/d", "d/d/c.txt"]);
    sim.check().unwrap_or_else(|e| panic!("{e}"));
}

/// One client removes a folder while the other, which synced a folder made in it, tags that
/// folder and makes one in it. The removal would take the synced folder along (§6.3), but the
/// new one keeps it alive: merged after all, it keeps its tag.
#[test]
fn regression_a_folder_kept_alive_in_a_removed_one_keeps_its_tag() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(1, CreateFolder { parent: 0, name: 0 }),
        plan(1),
        step(1),
        act(1, Delete { node: 0 }),
        plan(0),
        step(0),
        plan(1),
        act(0, CreateFolder { parent: 2, name: 0 }),
        plan(0),
        step(0),
        act(0, Tag { node: 1, tag: 4 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        step(1),
        act(
            0,
            CreateFolder {
                parent: 10,
                name: 0,
            },
        ),
    ];
    if let Err(e) = run_with(2, Some(1), |sim| sim, &events) {
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

/// The computer deleting a folder stops syncing a file in it before it removed the file: the
/// folder's removal took the file along (written before it), so it is removed, though no
/// device marks it so.
#[test]
fn regression_a_file_taken_along_and_no_longer_synced_is_removed() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(3, CreateFolder { parent: 0, name: 5 }),
        plan(3),
        act(0, Phone { op: 0, pick: 0 }),
        act(3, CreateFile { parent: 6, name: 0 }),
        step(3),
        plan(3),
        step(3),
        act(3, ToggleIgnore),
        plan(3),
        step(3),
        step(3),
    ];
    if let Err(e) = run_with(3, None, Sim::with_new_role_devices, &events) {
        panic!("{e}");
    }
}

/// One client edits a file and deletes the folder it moved it into; the other, which never
/// had that edit, only renamed the file meanwhile. The folder's removal took the edited version
/// along: the rename doesn't write over it (it stays, with the folder, in Recently deleted), the
/// renamed file goes up as a new item.
#[test]
fn regression_a_rename_doesnt_write_over_a_version_taken_along() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(1, CreateJunk { parent: 0 }),
        act(
            1,
            Move {
                node: 0,
                parent: 0,
                name: 3,
            },
        ),
        plan(1),
        step(1),
        act(1, CreateFolder { parent: 0, name: 0 }),
        act(1, Edit { file: 0 }),
        plan(0),
        act(0, CreateFile { parent: 0, name: 0 }),
        step(0),
        act(
            1,
            Move {
                node: 6,
                parent: 0,
                name: 0,
            },
        ),
        plan(1),
        act(0, CreateJunk { parent: 0 }),
        step(1),
        step(1),
        act(1, Delete { node: 1 }),
        act(1, CreateFile { parent: 0, name: 0 }),
        plan(1),
        step(1),
        act(
            0,
            Move {
                node: 0,
                parent: 0,
                name: 4,
            },
        ),
        step(1),
    ];
    if let Err(e) = run_with(2, Some(1), |sim| sim, &events) {
        panic!("{e}");
    }
}

/// A folder is written into another after every client began to ignore that one, and after it
/// was removed: written after the removal, it stays; its folder, ignored everywhere, isn't any
/// client's to bring back, so it goes to the top, where it stays visible.
#[test]
fn regression_a_folder_written_into_an_ignored_removed_one_stays_visible() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let events = [
        act(
            2,
            CreateFolder {
                parent: 0,
                name: 13,
            },
        ),
        plan(2),
        act(2, CreateFolder { parent: 0, name: 0 }),
        step(2),
        plan(0),
        act(0, Edit { file: 0 }),
        plan(2),
        step(0),
        act(2, ToggleIgnore),
        act(0, ToggleIgnore),
        plan(0),
        step(0),
        step(2),
    ];
    if let Err(e) = run(3, &events) {
        panic!("{e}");
    }
    if let Err(e) = run_with(3, None, Sim::with_role_folder, &events) {
        panic!("{e}");
    }
}

/// Scripted races around a folder's delete (node schema, Recently deleted): the server's time
/// decides what went with the folder.
mod folder_delete_races {
    use super::*;
    use Action::*;
    use hbsync_types::NodeId;

    // Not "d": the odd clients ignore it.
    const D: usize = 1; // "b"
    const A: usize = 0; // "a"
    const B: usize = 3; // "c.txt"

    fn act(sim: &mut Sim, client: usize, action: Action) {
        sim.run(&[Event::Act { client, action }]);
    }

    fn plan(sim: &mut Sim, client: usize) {
        sim.run(&[Event::Plan { client }]);
    }

    fn steps(sim: &mut Sim, client: usize, n: usize) {
        sim.run(&vec![Event::Step { client }; n]);
    }

    fn settle(sim: &mut Sim) {
        assert!(sim.quiesce(30), "no convergence");
        sim.check().unwrap_or_else(|e| panic!("{e}"));
    }

    /// What a create's `parent` is to pick folder `path` on client `c`.
    fn folder(sim: &Sim, c: usize, path: &str) -> usize {
        let node = if path.is_empty() {
            NodeId::ROOT
        } else {
            sim.item(c, path).expect(path)
        };
        sim.pick(c, node, true).expect(path)
    }

    /// What a `Delete` or `Edit` picks for `path` on client `c`.
    fn item(sim: &Sim, c: usize, path: &str) -> usize {
        let node = sim.item(c, path).expect(path);
        sim.pick(c, node, false).expect(path)
    }

    /// The index `Edit` picks for file `path` on client `c` (files only, by id).
    fn pick_file(sim: &Sim, c: usize, path: &str) -> usize {
        sim.file_pick(c, sim.item(c, path).expect(path))
            .expect(path)
    }

    /// Folder `b` with `b/a` in it, synced on both clients.
    fn synced_folder() -> Sim {
        let mut sim = Sim::new(2);
        let root = folder(&sim, 0, "");
        act(
            &mut sim,
            0,
            CreateFolder {
                parent: root,
                name: D,
            },
        );
        let d = folder(&sim, 0, "b");
        act(&mut sim, 0, CreateFile { parent: d, name: A });
        settle(&mut sim);
        sim
    }

    /// One client adds `b/c.txt` and deletes `b` while the other hasn't downloaded `b/c.txt`
    /// yet, and plans in the middle of the delete: `b/c.txt` was written before the folder's
    /// removal, so it went with it, and the folder doesn't come back.
    #[test]
    fn a_file_not_downloaded_yet_doesnt_bring_its_folder_back() {
        let mut sim = synced_folder();
        let d = folder(&sim, 0, "b");
        act(&mut sim, 0, CreateFile { parent: d, name: B });
        plan(&mut sim, 0);
        steps(&mut sim, 0, 5);
        let photos = sim.item(0, "b").expect("b");
        let node = item(&sim, 0, "b");
        act(&mut sim, 0, Delete { node });
        plan(&mut sim, 0);
        steps(&mut sim, 0, 1); // the folder first
        assert!(sim.is_removed(photos));
        plan(&mut sim, 1);
        steps(&mut sim, 1, 10);
        settle(&mut sim);
        assert_eq!(sim.server_paths(), Vec::<String>::new());
    }

    /// One client edits `b/a` and uploads it before the other, not knowing, deletes `b`: the
    /// edit came before the delete, so it went with the folder, into Recently deleted with its
    /// content.
    #[test]
    fn an_edit_before_the_delete_goes_with_the_folder() {
        let mut sim = synced_folder();
        let a = sim.item(1, "b/a").expect("b/a");
        let file = pick_file(&sim, 1, "b/a");
        act(&mut sim, 1, Edit { file });
        plan(&mut sim, 1);
        let node = item(&sim, 0, "b");
        act(&mut sim, 0, Delete { node });
        plan(&mut sim, 0); // before the edit is on the drive
        steps(&mut sim, 1, 5);
        steps(&mut sim, 0, 5);
        settle(&mut sim);
        assert_eq!(sim.server_paths(), Vec::<String>::new());
        assert!(sim.is_removed(a), "the edit is in Recently deleted");
    }

    /// One client deletes `b`; the other, not knowing, edits `b/a` after that: the edit came
    /// after the delete, so the file stays, and brings its folder back.
    #[test]
    fn an_edit_after_the_delete_keeps_the_file_and_its_folder() {
        let mut sim = synced_folder();
        let node = item(&sim, 0, "b");
        act(&mut sim, 0, Delete { node });
        plan(&mut sim, 0);
        steps(&mut sim, 0, 1); // the folder first
        let file = pick_file(&sim, 1, "b/a");
        act(&mut sim, 1, Edit { file });
        plan(&mut sim, 1);
        steps(&mut sim, 1, 5);
        settle(&mut sim);
        assert_eq!(sim.server_paths(), ["b", "b/a"]);
    }

    /// A client that never saw `b` plans in the middle of its delete: `b/c.txt`, written before
    /// the removal, went with it. It isn't moved to the top, nor is `b` brought back.
    #[test]
    fn a_folder_never_seen_doesnt_leave_its_files_at_the_top() {
        let mut sim = Sim::new(2);
        let root = folder(&sim, 0, "");
        act(
            &mut sim,
            0,
            CreateFolder {
                parent: root,
                name: D,
            },
        );
        let d = folder(&sim, 0, "b");
        act(&mut sim, 0, CreateFile { parent: d, name: B });
        plan(&mut sim, 0);
        steps(&mut sim, 0, 5);
        let node = item(&sim, 0, "b");
        act(&mut sim, 0, Delete { node });
        plan(&mut sim, 0);
        steps(&mut sim, 0, 1);
        plan(&mut sim, 1);
        steps(&mut sim, 1, 10);
        settle(&mut sim);
        assert_eq!(sim.server_paths(), Vec::<String>::new());
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

/// A synced folder is moved into another and tagged; a client that had it at its old place
/// only by a download that landed after that, removes the other folder: unchanged here, the
/// folder goes with it, into Recently deleted with its tag (the drive's), on no computer.
#[test]
fn regression_a_folder_taken_along_goes_with_its_tag() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let mut sim = Sim::new(3).with_new_role_devices();
    sim.run(&[act(1, CreateFolder { parent: 0, name: 6 })]);
    let folder = sim.item_named(1, ".txt").expect("the folder");
    sim.run(&[
        act(0, CreateFolder { parent: 0, name: 8 }),
        plan(0),
        step(0),
        plan(1),
        act(0, Delete { node: 0 }),
        step(1),
        step(1),
        act(
            1,
            Move {
                node: 2,
                parent: 1,
                name: 0,
            },
        ),
        act(1, Tag { node: 8, tag: 0 }),
        plan(1),
        plan(0),
        step(1),
        step(0),
        step(0),
    ]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    assert!(sim.is_removed(folder), "{:?}", sim.server_paths());
    assert!(
        sim.removed_tags(folder)
            .iter()
            .any(|t| t.starts_with("Red")),
        "Recently deleted keeps Red"
    );
}

/// A synced folder is moved into another and tagged on one computer; the other, which hadn't
/// seen that yet, removes the other folder, taking it along, and renames its copy: the rename
/// brings it back with the tag the drive had, which this computer never saw (§11).
#[test]
fn regression_a_folder_brought_back_from_a_removal_keeps_the_drives_tag() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let mut sim = Sim::new(3).with_new_role_devices();
    sim.run(&[
        act(1, CreateFolder { parent: 0, name: 6 }),
        act(0, CreateFolder { parent: 0, name: 8 }),
    ]);
    assert!(sim.quiesce(10));
    let folder = sim.item_named(1, ".txt").expect("the folder");
    let other = sim.item(0, "a").expect("the other folder");
    let pick = |sim: &Sim, c, n, parent| sim.pick(c, n, parent).expect("picked");
    let (f1, a1, a0) = (
        pick(&sim, 1, folder, false),
        pick(&sim, 1, other, true),
        pick(&sim, 0, other, false),
    );
    sim.run(&[
        act(
            1,
            Move {
                node: f1,
                parent: a1,
                name: 1,
            },
        ),
        act(1, Tag { node: f1, tag: 0 }),
        act(0, Delete { node: a0 }),
        plan(1),
        plan(0),
        step(1),
        step(0),
    ]);
    // Client 0 renames its copy, which it still has at the top.
    let (f0, top) = (
        pick(&sim, 0, folder, false),
        pick(&sim, 0, NodeId::ROOT, true),
    );
    sim.run(&[act(
        0,
        Move {
            node: f0,
            parent: top,
            name: 1,
        },
    )]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    let red =
        |tags: Option<Vec<String>>| tags.is_some_and(|t| t.iter().any(|t| t.starts_with("Red")));
    assert!(
        red(sim.server_tags("b")),
        "the drive keeps Red: {:?}",
        sim.server_paths()
    );
    // The Mac and the Linux computer; Windows holds no tags (the drive keeps them, §11).
    for c in 0..2 {
        assert!(red(sim.local_tags(c, "b")), "computer {c} keeps Red");
    }
}

/// Documents removes a folder, then a file in it; in between the role folder is deleted and
/// the folder restored to the top, and the file's removal decided in Documents still runs.
#[test]
fn regression_a_restore_during_a_folder_delete_isnt_a_write_outside() {
    use Action::*;
    let act = |client, action| Event::Act { client, action };
    let plan = |client| Event::Plan { client };
    let step = |client| Event::Step { client };
    let mut sim = Sim::new(3).with_new_role_devices();
    sim.run(&[
        act(3, CreateFile { parent: 0, name: 0 }),
        act(4, CreateFolder { parent: 0, name: 0 }),
        act(4, CreateFile { parent: 2, name: 0 }),
    ]);
    let file = sim.item(4, "a/a").expect("Documents' a/a");
    sim.run(&[
        act(0, CreateFile { parent: 0, name: 1 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        plan(4),
        act(4, CreateJunk { parent: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        step(4),
        step(4),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        act(4, Delete { node: 3 }),
        act(0, CreateFile { parent: 0, name: 0 }),
        plan(4),
        step(4),
        act(0, DeleteRoleFolder),
        act(0, Restore { node: 0 }),
        step(4),
    ]);
    assert!(sim.quiesce(30));
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    let paths = sim.server_paths();
    assert!(
        paths.iter().any(|p| p == "a"),
        "the restored folder: {paths:?}"
    );
    assert!(!paths.iter().any(|p| p == "a/a"), "{paths:?}");
    assert!(sim.is_removed(file), "the file is in Recently deleted");
}

/// A folder restored from Recently deleted, made here before its RemoteCreate runs: another
/// device restores it first, with a tag removed. The folder made here has the removed version
/// as its base, so the removal wins (with no base, the tag read as added here, and came back).
#[test]
fn regression_a_tag_removed_while_a_restored_folder_has_no_base() {
    use hbsync_sim::Action::*;
    use hbsync_sim::Event::*;
    let events = [
        Act {
            client: 1,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Act {
            client: 1,
            action: Tag { node: 0, tag: 9 },
        },
        Plan { client: 1 },
        Step { client: 1 },
        Act {
            client: 1,
            action: CreateFolder { parent: 1, name: 5 },
        },
        Plan { client: 0 },
        Act {
            client: 1,
            action: CreateFolder { parent: 7, name: 0 },
        },
        Step { client: 0 },
        Act {
            client: 1,
            action: Move {
                node: 3,
                parent: 13,
                name: 3,
            },
        },
        Act {
            client: 0,
            action: CreateFile { parent: 2, name: 0 },
        },
        Plan { client: 1 },
        Plan { client: 0 },
        Step { client: 1 },
        Step { client: 0 },
        Plan { client: 1 },
        Step { client: 1 },
        Act {
            client: 0,
            action: CreateJunk { parent: 0 },
        },
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Act {
            client: 0,
            action: Tag { node: 0, tag: 5 },
        },
    ];
    run_with(3, None, Sim::with_split_downloads, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Client 0 deletes a tagged folder whose drive copy holds a file it never had: the delete loses,
/// and the folder is made again, but the crash comes before its tags are written and its commit.
/// The user then renames it: away from the record's place, the scan finds it as a new folder
/// (the engine's write-ahead record pairs only what is at its place), not the node with no tags.
/// The simulator kept it as the node, its missing tag read as a removal (`cc 03016076…`).
#[test]
fn regression_a_folder_made_again_and_moved_before_recovery_is_a_new_folder() {
    use hbsync_sim::Action::*;
    use hbsync_sim::Event::*;
    let events = [
        Act {
            client: 1,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Act {
            client: 1,
            action: Tag { node: 0, tag: 8 },
        },
        Act {
            client: 1,
            action: CreateFile { parent: 6, name: 0 },
        },
        Plan { client: 1 },
        Step { client: 1 },
        Plan { client: 0 },
        Step { client: 0 },
        Step { client: 1 },
        Act {
            client: 0,
            action: Delete { node: 0 },
        },
        Plan { client: 0 },
        Crash { client: 0 },
        Act {
            client: 0,
            action: Move {
                node: 0,
                parent: 7,
                name: 1,
            },
        },
    ];
    run_with(3, None, Sim::with_split_downloads, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Client 1 downloads a version client 0 tagged Work; the crash comes once it is in place,
/// before its commit, and the user edits it. Client 0 removes Work meanwhile. Recovery can't
/// tell the edited item from the file there before (its bytes are neither), so it drops the
/// record and the old base stands: the Work the download wrote reads as client 1 adding it,
/// and comes back (the known limit, sync semantics §8, §11; `cc 86011d5e…`). Nothing is lost:
/// the edit goes to a conflict copy.
#[test]
fn regression_a_tag_a_crashed_download_wrote_may_come_back_after_an_edit() {
    use hbsync_sim::Action::*;
    use hbsync_sim::Event::*;
    let events = [
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 2 },
        },
        Plan { client: 0 },
        Step { client: 0 },
        Act {
            client: 0,
            action: Tag { node: 0, tag: 1 },
        },
        Act {
            client: 0,
            action: Edit { file: 0 },
        },
        Plan { client: 1 },
        Step { client: 1 },
        Plan { client: 0 },
        Step { client: 0 },
        Act {
            client: 0,
            action: Tag { node: 0, tag: 9 },
        },
        Step { client: 1 },
        Plan { client: 1 },
        Step { client: 1 },
        Crash { client: 1 },
        Act {
            client: 1,
            action: Edit { file: 0 },
        },
    ];
    run_with(3, None, Sim::with_split_downloads, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Client 2 deletes a folder (an ignore rule takes it off the drive) while clients 0 and 1 tag
/// it red, neither synced yet. Client 0 brings it back (a create of the removed node, with its
/// red) and then removes red. Client 1 never saw the delete: its red goes up against the
/// version back, as any add, and red is back by right. The checker took the restore for
/// settling every add the removed version didn't hold, client 1's unsent one too, and read
/// red as client 0's removal coming back (`cc 6b8411dc…`).
#[test]
fn regression_an_add_not_sent_up_outlives_a_restore() {
    use hbsync_sim::Action::*;
    use hbsync_sim::Event::*;
    let events = [
        Act {
            client: 2,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Act {
            client: 2,
            action: Move {
                node: 0,
                parent: 13,
                name: 13,
            },
        },
        Act {
            client: 1,
            action: ToggleIgnore,
        },
        Plan { client: 2 },
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 6 },
        },
        Plan { client: 0 },
        Step { client: 0 },
        Step { client: 2 },
        Plan { client: 1 },
        Plan { client: 0 },
        Step { client: 1 },
        Act {
            client: 1,
            action: CreateFolder { parent: 0, name: 0 },
        },
        Act {
            client: 2,
            action: ToggleIgnore,
        },
        Step { client: 1 },
        Plan { client: 2 },
        Step { client: 0 },
        Act {
            client: 0,
            action: Tag { node: 0, tag: 4 },
        },
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Act {
            client: 1,
            action: Tag { node: 3, tag: 12 },
        },
        Act {
            client: 0,
            action: CreateFile { parent: 0, name: 0 },
        },
        Step { client: 2 },
        Step { client: 2 },
        Plan { client: 0 },
        Step { client: 0 },
        Act {
            client: 0,
            action: Tag { node: 3, tag: 4 },
        },
    ];
    run(3, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
}
