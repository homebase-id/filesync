use hbsync_sim::{Action, Event, Sim};
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
        2 => (i(), i()).prop_map(|(node, tag)| Action::Tag { node, tag }),
        1 => (i(), i()).prop_map(|(node, tag)| Action::Colour { node, tag }),
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

fn run(clients: usize, events: &[Event]) -> Result<(), TestCaseError> {
    run_with(clients, None, events)
}

/// Runs `events` with the mass-delete guard at `guard` (its minimum), then lets every client
/// settle (confirming what the guard holds, with the guard on) and checks the outcome.
fn run_with(clients: usize, guard: Option<usize>, events: &[Event]) -> Result<(), TestCaseError> {
    let outcome = |verbose: bool| {
        let mut sim = Sim::new(clients);
        sim.guard = guard;
        sim.verbose = verbose;
        sim.run(events);
        let result = if sim.quiesce(30) {
            sim.check()
        } else {
            Err("no convergence after 30 rounds".into())
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
    #![proptest_config(ProptestConfig { cases: 2000, ..ProptestConfig::default() })]

    #[test]
    fn two_clients_converge_without_loss(events in proptest::collection::vec(event(2), 0..80)) {
        run(2, &events)?;
    }

    #[test]
    fn three_clients_converge_without_loss(events in proptest::collection::vec(event(3), 0..120)) {
        run(3, &events)?;
    }

    /// With the mass-delete guard on (a low limit, so it trips often), no plan deletes more
    /// than it allows unless the user confirmed those very deletes, and once the user confirms
    /// what is held, the clients still converge without loss.
    #[test]
    fn guarded_clients_delete_only_what_was_confirmed(
        events in proptest::collection::vec(guarded_event(2), 0..80),
        min in 1usize..3,
    ) {
        run_with(2, Some(min), &events)?;
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
    if let Err(e) = run_with(2, Some(1), &events) {
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
    events.push(act(1, Purge));
    run(2, &events)
        .map_err(|e| e.to_string())
        .unwrap_or_else(|e| panic!("{e}"));
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
