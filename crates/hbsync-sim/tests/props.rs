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

fn run(clients: usize, events: &[Event]) -> Result<(), TestCaseError> {
    let outcome = |verbose: bool| {
        let mut sim = Sim::new(clients);
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
