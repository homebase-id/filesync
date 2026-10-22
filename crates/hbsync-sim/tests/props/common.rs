//! What the properties and regressions share: the event strategies, the setups, and running a
//! simulation to a verdict.

use hbsync_sim::{Action, Event, Sim};
use hbsync_types::NodeId;
use proptest::prelude::*;

pub(crate) fn action() -> impl Strategy<Value = Action> + Clone {
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
        1 => i().prop_map(|file| Action::MoveAcross { file }),
        1 => i().prop_map(|op| Action::Meanwhile { op }),
    ]
}

/// The users' actions, with another device's change of a node pulled between a local
/// operation's check and its commit ([`Action::PullMidLocal`]) now and then.
pub(crate) fn pulling_action() -> impl Strategy<Value = Action> + Clone {
    prop_oneof![
        6 => action(),
        1 => (0usize..5, 0usize..3).prop_map(|(on, change)| Action::PullMidLocal { on, change }),
    ]
}

/// The users' actions, with deletes another device stops half way and purges, more often.
pub(crate) fn stopped_delete_action() -> impl Strategy<Value = Action> + Clone {
    let i = || 0usize..16;
    prop_oneof![
        6 => action(),
        1 => (i(), i()).prop_map(|(pick, done)| Action::DeleteStopped { pick, done }),
        1 => Just(Action::Purge),
    ]
}

/// The users' actions, with folders another app deletes outright (their contents left with no
/// folder), more often.
pub(crate) fn orphaning_action() -> impl Strategy<Value = Action> + Clone {
    prop_oneof![
        6 => action(),
        1 => (0usize..16).prop_map(|pick| Action::DeleteOutright { pick }),
        1 => (0usize..16, 0usize..16).prop_map(|(pick, file)| Action::IntoFile { pick, file }),
        1 => (0usize..16).prop_map(|name| Action::NewerFolder { name }),
    ]
}

/// The users' actions, with deletes, and apps writing to what a client moved aside to delete
/// (sync semantics §7), more often.
pub(crate) fn deleting_action() -> impl Strategy<Value = Action> + Clone {
    let i = || 0usize..16;
    prop_oneof![
        6 => action(),
        2 => i().prop_map(|node| Action::Delete { node }),
        2 => i().prop_map(|pick| Action::EditAside { pick }),
        1 => (i(), i()).prop_map(|(pick, name)| Action::CreateInAside { pick, name }),
        1 => (i(), i()).prop_map(|(pick, done)| Action::DeleteStopped { pick, done }),
    ]
}

pub(crate) fn event(clients: usize) -> impl Strategy<Value = Event> {
    events_of(clients, action())
}

/// Events with the users' `actions`.
pub(crate) fn events_of(
    clients: usize,
    actions: impl Strategy<Value = Action> + Clone,
) -> impl Strategy<Value = Event> {
    let c = move || 0..clients;
    prop_oneof![
        4 => (c(), actions).prop_map(|(client, action)| Event::Act { client, action }),
        1 => c().prop_map(|client| Event::Plan { client }),
        3 => c().prop_map(|client| Event::Step { client }),
    ]
}

/// Events with the user confirming held deletes now and then.
pub(crate) fn guarded_event(clients: usize) -> impl Strategy<Value = Event> {
    prop_oneof![
        8 => event(clients),
        1 => (0..clients).prop_map(|client| Event::Confirm { client }),
        1 => (0..clients).prop_map(|client| Event::PutBack { client }),
    ]
}

/// Events with a client dying now and then ([`Event::Crash`]).
pub(crate) fn crashing_event(clients: usize) -> impl Strategy<Value = Event> {
    with_crashes(clients, event(clients))
}

/// `events`, with a client dying now and then ([`Event::Crash`]).
pub(crate) fn with_crashes(
    clients: usize,
    events: impl Strategy<Value = Event>,
) -> impl Strategy<Value = Event> {
    prop_oneof![
        8 => events,
        1 => (0..clients).prop_map(|client| Event::Crash { client }),
    ]
}

pub(crate) fn run(clients: usize, events: &[Event]) -> Result<(), TestCaseError> {
    run_with(clients, None, |sim| sim, events)
}

/// Runs `events` with the mass-delete guard at `guard` (its minimum), on a simulation `setup`
/// prepares, then lets every client settle (confirming what the guard holds, with the guard
/// on) and checks the outcome.
pub(crate) fn run_with(
    clients: usize,
    guard: Option<usize>,
    setup: impl Fn(Sim) -> Sim,
    events: &[Event],
) -> Result<(), TestCaseError> {
    run_settled(clients, guard, setup, |sim| sim.quiesce(30), events)
}

/// [`run_with`], the clients settling as `settle` lets them (whether they did).
pub(crate) fn run_settled(
    clients: usize,
    guard: Option<usize>,
    setup: impl Fn(Sim) -> Sim,
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

/// 2000 cases, unless `PROPTEST_CASES` says otherwise (a set `cases` would override it).
pub(crate) fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000)
}

/// A simulation's setup, and the mass-delete guard's minimum if it is on.
pub(crate) type Setup = (fn(Sim) -> Sim, Option<usize>);

/// The setups [`a_run_replays_the_same`] picks from, each with the mass-delete guard (its
/// minimum) or not: every kind of run a property makes.
pub(crate) const SETUPS: &[Setup] = &[
    (Sim::with_split_downloads, None),
    (Sim::with_refusals, None),
    (Sim::with_wrong_not_theres, None),
    (Sim::with_role_device, None),
    (Sim::with_new_role_devices, Some(1)),
    (|sim| sim, Some(2)),
];

/// The setups [`the_engines_planning_path_plans_as_the_sim`] picks from: those that settle
/// (refusals and wrong "not there"s leave things held, judged by properties of their own).
pub(crate) const NARROW_SETUPS: &[Setup] = &[
    (Sim::with_split_downloads, None),
    (Sim::with_role_device, None),
    (Sim::with_new_role_devices, Some(1)),
    (|sim| sim, Some(2)),
];

/// Events of every kind the properties make: the users' actions, deletes another device stops
/// half way, folders another app deletes outright, confirmations and crashes.
pub(crate) fn any_event(clients: usize) -> impl Strategy<Value = Event> {
    let actions = prop_oneof![
        1 => stopped_delete_action(),
        1 => orphaning_action(),
        1 => deleting_action(),
    ];
    let c = move || 0..clients;
    prop_oneof![
        8 => events_of(clients, actions),
        1 => c().prop_map(|client| Event::Confirm { client }),
        1 => c().prop_map(|client| Event::Crash { client }),
    ]
}

/// Events with files still being written at a plan ([`Action::EditWriting`]), and folders
/// deleted on other devices, outright and by delete.
pub(crate) fn writing_event(clients: usize) -> impl Strategy<Value = Event> {
    let i = || 0usize..16;
    let actions = prop_oneof![
        4 => action(),
        2 => i().prop_map(|file| Action::EditWriting { file }),
        1 => i().prop_map(|file| Action::Lock { file }),
        1 => i().prop_map(|pick| Action::DeleteOutright { pick }),
        1 => (i(), i()).prop_map(|(pick, file)| Action::IntoFile { pick, file }),
    ];
    events_of(clients, actions)
}

/// Client 0 and the other devices, with folder "b" holding "b/c.txt" and "b/a/c.txt", all
/// synced; and the ids of b, b/a, b/c.txt and b/a/c.txt.
pub(crate) fn stopped_delete_setup() -> (Sim, [NodeId; 4]) {
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

/// Runs scripted `events` on `clients` clients, settles them and checks the outcome: the tag
/// adds lost to a known limit of the engine, as [`Sim::tags_lost_to_known_limits`] names them.
pub(crate) fn known_limits_of(clients: usize, events: &[Event]) -> Vec<String> {
    let mut sim = Sim::new(clients);
    sim.run(events);
    assert!(sim.quiesce(30), "no convergence");
    sim.check().unwrap_or_else(|e| panic!("{e}"));
    sim.tags_lost_to_known_limits()
}
