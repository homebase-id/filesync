//! The properties: random runs of every kind, each judged by the oracle.

use crate::common::*;
use hbsync_sim::{Event, Sim};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: cases(),
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

    /// Another device's change of a node (an edit, a removal, a restore) is pulled between a
    /// local operation's check and its commit (`Action::PullMidLocal`): the commit is against
    /// the drive as it is then, the change is still planned, and the clients converge without
    /// loss.
    #[test]
    fn a_pull_between_a_local_check_and_its_commit_loses_nothing(
        events in proptest::collection::vec(events_of(3, pulling_action()), 0..120)
    ) {
        run(3, &events)?;
    }

    /// The drive refuses some requests as they are, for good (a write it won't take, a
    /// download whose bytes don't match at a version): each client records a refusal and
    /// doesn't send the same operation again (`hbsync_engine::refused`). The record never hides
    /// a later change: whatever a client holds back is refused by the drive as it is now (a
    /// local edit, a new version on the drive, a rename or a delete is another request), and
    /// everything else converges without loss.
    #[test]
    fn refusals_never_hide_a_later_change(
        events in proptest::collection::vec(event(2), 0..80),
    ) {
        run_settled(2, None, |sim| sim.with_refusals(), |sim| sim.quiesce_still(30), &events)?;
    }

    /// The identity says now and then that a live file isn't there (its own 404, a hiccup;
    /// sync semantics §8b): a file the identity said gone once but is live is never taken as
    /// gone for good, nor deleted here, while files deleted for good still go; the clients
    /// converge without loss.
    #[test]
    fn a_live_file_said_gone_once_is_never_deleted(
        events in proptest::collection::vec(event(2), 0..80),
    ) {
        // Checked also when they don't settle: a file taken as gone keeps them from it.
        let settle = |sim: &mut Sim| sim.quiesce(30) || sim.took_a_live_file_as_gone();
        run_settled(2, None, |sim| sim.with_wrong_not_theres(), settle, &events)?;
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

    /// With the mass-delete guard on (a low limit, so it trips often), no plan removes more from
    /// the drive than it allows unless the user confirmed those very removals, none holds a
    /// removal from elsewhere short of every synced file, and whether the user confirms what is
    /// held or puts it back (sync semantics §7), the clients still converge without loss.
    #[test]
    fn guarded_clients_delete_only_what_was_confirmed(
        events in proptest::collection::vec(guarded_event(2), 0..80),
        min in 1usize..3,
    ) {
        run_with(2, Some(min), |sim| sim, &events)?;
    }

    /// The same with put backs that go wrong (sync semantics §7): a file deleted for good
    /// between a put back's read of the drive and its restore, a restore that stops half way.
    /// A put back loses nothing, and what it found gone for good goes up again from here: the
    /// next plan holds nothing of it again but what still waits in Recently deleted.
    #[test]
    fn put_backs_that_go_wrong_lose_nothing(
        events in proptest::collection::vec(guarded_event(2), 0..80),
    ) {
        run_with(2, Some(WIPED_MIN), with_a_folder_wiped, &events)?;
    }

    /// The same with clients that crash now and then: a crash forgets the user's answers and
    /// what they were shown, as a new engine does; what the guard holds is held and asked
    /// about again, and nothing goes through unconfirmed.
    #[test]
    fn guarded_clients_that_crash_ask_again(
        events in proptest::collection::vec(with_crashes(2, guarded_event(2)), 0..80),
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

    /// Another device deletes a folder and stops half way, never to come back (any folder,
    /// Documents and what is in it too, beside a computer syncing it), while the users work and
    /// the daily purge runs: the purge removes what the delete took along, once the folder is
    /// kept long enough, and deletes it for good 30 days on, never a live node, never what was
    /// written after the delete. The clients converge without loss.
    #[test]
    fn stopped_deletes_and_the_purge_converge_without_loss(
        events in proptest::collection::vec(events_of(4, stopped_delete_action()), 0..120)
    ) {
        run_with(3, None, Sim::with_role_device, &events)?;
    }

    /// Another app deletes folders outright, leaving what is in them with no folder, or moves a
    /// node into a file, while the users work (in the role folder too) and the purge runs: the
    /// main folders put what is left, and what is in a file, into Recovered files, made by the
    /// first and joined by the others, and the clients converge without loss.
    #[test]
    fn orphans_are_recovered_without_loss(
        events in proptest::collection::vec(events_of(4, orphaning_action()), 0..120)
    ) {
        run_with(3, None, Sim::with_role_device, &events)?;
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

    /// Items deleted on one device are deleted outright on the others (sync semantics §7):
    /// moved aside, checked, unlinked. Meanwhile apps that had a file open write to it there,
    /// and save new files into a folder there, and users save at the names they left. Every
    /// such write is kept (an edit wins), nothing is unlinked that isn't the version its base
    /// records, nothing stays aside, and the clients converge without loss.
    #[test]
    fn remote_deletes_never_lose_an_edit(
        events in proptest::collection::vec(events_of(3, deleting_action()), 0..120)
    ) {
        run(3, &events)?;
    }

    /// As [`remote_deletes_never_lose_an_edit`], with the clients dying at any step of a delete
    /// (right after the move aside, part way through the unlinks, before the commit), and of a
    /// download into another folder (its old version aside, before the new one is in place,
    /// with its commit lost, or committed and the old one not unlinked yet), apps writing to
    /// what is aside meanwhile. Recovery decides what is left aside as the executor's does;
    /// nothing is lost.
    #[test]
    fn a_crash_anywhere_in_a_delete_loses_nothing(
        events in proptest::collection::vec(with_crashes(3, events_of(3, deleting_action())), 0..120)
    ) {
        run_with(3, None, Sim::with_split_downloads, &events)?;
    }

    /// Each client plans its nodes in pages of 1 to 4 (`Sim::with_small_pages`), as the engine
    /// plans a dirty set of more than a page (20,000 nodes), while folders are deleted, stopped
    /// half way and purged: what a page leaves to another holds its folders, and a folder held
    /// so goes with its subtree as a page of its own. The clients converge without loss, and no
    /// deleted folder comes back (a child alive as context kept it alive: review 14 R1).
    #[test]
    fn clients_planning_in_small_pages_converge_without_loss(
        size in 1usize..5,
        events in proptest::collection::vec(events_of(3, stopped_delete_action()), 0..100),
    ) {
        run_with(3, None, |sim| sim.with_small_pages(size), &events)?;
    }

    /// The users act (folders deleted, stopped half way, purged), the clients syncing all now
    /// and then: in pages of 1 to 4 they end with as many folders and files on the drive as
    /// in one page, and as many files in Recently deleted apart from their folders (a name
    /// clash's numbered name may go to the other node: no-loss checks the bytes). A delete
    /// spanning pages
    /// that brought its folders back (a child alive as context kept them alive, review 14 R1)
    /// converged all the same, on folders no one kept: only the comparison shows it. Without
    /// what depends on the order of writes: what another device does while a client writes
    /// (`Meanwhile`), what picks from Recently deleted by its order (removal times), what a
    /// trash keeps on its device (junk, ignored items: it gets new folders, or keeps its old
    /// one, as the folders above it go before or after it, and a folder too big for a page
    /// goes as its folders first), and edits on more than one device (a conflict copy's id
    /// comes from the version it forked at, which the number of writes before sets, and an
    /// action picks its node by id order).
    #[test]
    fn planning_in_small_pages_ends_as_planning_whole(
        size in 1usize..5,
        acts in proptest::collection::vec(
            (0usize..3, stopped_delete_action(), proptest::bool::weighted(0.25)).prop_map(
                |(client, action, sync)| {
                    use hbsync_sim::Action::*;
                    match action {
                        // Order-dependent: a purge instead.
                        Meanwhile { .. }
                        | Restore { .. }
                        | EmptyItem { .. }
                        | EmptyListed
                        | ListDeleted
                        | EditTaken { .. }
                        | ToggleIgnore
                        | CreateJunk { .. } => (client, Purge, sync),
                        // Edits on one device.
                        Edit { .. } | EditWriting { .. } | Lock { .. } => (0, action, sync),
                        _ => (client, action, sync),
                    }
                },
            ),
            0..60,
        ),
    ) {
        let end = |paged: bool| -> Result<(usize, usize, usize), TestCaseError> {
            // The same dirty set as the engine keeps (`with_small_pages`), in one page or many.
            let pages = if paged { size } else { 1_000_000 };
            let mut sim = Sim::new(3).without_ignore_rules().with_small_pages(pages);
            for &(client, action, sync) in &acts {
                sim.run(&[Event::Act { client, action }]);
                if sync {
                    prop_assert!(sim.quiesce(30), "no convergence (in pages: {paged})");
                }
            }
            prop_assert!(sim.quiesce(30), "no convergence (in pages: {paged})");
            if let Err(e) = sim.check() {
                return Err(TestCaseError::fail(format!("in pages: {paged}: {e}")));
            }
            // How many folders and files are there, whatever their names (a clash's numbered
            // name may go to the other node), and how many files were removed apart from their
            // folders, listed so in Recently deleted or purged since (the purge's pick depends
            // on how many writes came after a removal, which pages change).
            let (folders, files) = sim.server_counts();
            Ok((folders, files, sim.removed_files_apart()))
        };
        prop_assert_eq!(end(true)?, end(false)?);
    }
}

proptest! {
    // A hundredth of the cases, at least one: every fourth plan is planned three more times,
    // over a state DB it fills (about 0.5 s a case in release: 20 at the default, 1500 at
    // 150000).
    #![proptest_config(ProptestConfig {
        cases: (cases() / 100).max(1),
        ..ProptestConfig::default()
    })]

    /// The engine plans from its store, from narrow views of what is dirty (`view::plan_narrow`);
    /// the sim plans from whole trees. Every fourth plan a main folder makes, its input filled into a
    /// state DB as the engine's store holds it, plans the same through the engine's views, and
    /// from narrow views the same as from whole folders for some of its nodes dirty.
    #[test]
    fn the_engines_planning_path_plans_as_the_sim(
        setup in 0..NARROW_SETUPS.len(),
        events in proptest::collection::vec(any_event(5), 0..80),
    ) {
        let (prepare, guard) = NARROW_SETUPS[setup];
        let mut sim = prepare(Sim::new(3)).with_narrow_check();
        sim.guard = guard;
        sim.run(&events);
        prop_assert!(sim.quiesce(30), "no convergence");
        if let Err(e) = sim.check() {
            prop_assert!(false, "{}", e);
        }
    }
}

proptest! {
    // A twentieth of the cases, at least one: a plan that holds deletes is planned twice more,
    // over a state DB it fills.
    #![proptest_config(ProptestConfig {
        cases: (cases() / 20).max(1),
        ..ProptestConfig::default()
    })]

    /// The engine takes held deletes out of the dirty set till the user answers
    /// (`hbsync_engine::held`), so they aren't planned again every sync: a plan without them
    /// plans as one with them (their deletes held), unless its view reaches them, and then the
    /// engine plans them again with it. Checked on every plan that holds deletes, with the
    /// guard tripping often; the clients converge without loss whatever the users answer.
    #[test]
    fn held_deletes_out_of_the_dirty_set_plan_alike(
        events in proptest::collection::vec(guarded_event(2), 0..80),
        min in 1usize..3,
    ) {
        run_with(2, Some(min), Sim::with_held_check, &events)?;
    }

    /// The same from a folder's removal elsewhere held as every synced file
    /// ([`with_a_folder_wiped`]): a held delete out of the dirty set from the first plan, so
    /// the plans that hold deletes are compared (rarely from an empty start).
    #[test]
    fn held_deletes_out_of_the_dirty_set_plan_alike_from_a_wipe(
        events in proptest::collection::vec(guarded_event(2), 0..80),
    ) {
        run_with(2, Some(WIPED_MIN), |sim| with_a_folder_wiped(sim.with_held_check()), &events)?;
    }
}

proptest! {
    /// A file still being written when its device plans (not hashed yet, its merge left for
    /// later) is where it is there, whatever the drive did with its node meanwhile: it keeps
    /// its folder alive against another device's delete of it, outright or not (its edits
    /// win), and keeps its name and tags; once hashed it syncs. Nothing is lost.
    #[test]
    fn a_file_being_written_keeps_its_folder(
        events in proptest::collection::vec(writing_event(3), 0..80),
    ) {
        run(3, &events)?;
    }
}

proptest! {
    // A hundredth of the cases, at least one, as the narrow property's.
    #![proptest_config(ProptestConfig {
        cases: (cases() / 100).max(1),
        ..ProptestConfig::default()
    })]

    /// [`a_file_being_written_keeps_its_folder`] on every setup, and its plans made through the
    /// engine's views as well ([`the_engines_planning_path_plans_as_the_sim`]): what waits for
    /// a file not hashed yet waits there as here, the role folders' too.
    #[test]
    fn a_file_being_written_waits_alike_on_the_engines_path(
        setup in 0..NARROW_SETUPS.len(),
        events in proptest::collection::vec(writing_event(5), 0..80),
    ) {
        let (prepare, guard) = NARROW_SETUPS[setup];
        let mut sim = prepare(Sim::new(3)).with_narrow_check();
        sim.guard = guard;
        sim.run(&events);
        prop_assert!(sim.quiesce(30), "no convergence");
        if let Err(e) = sim.check() {
            prop_assert!(false, "{}", e);
        }
    }
}

proptest! {
    // A twentieth of the cases, at least one: each runs twice with the trace on.
    #![proptest_config(ProptestConfig {
        cases: (cases() / 20).max(1),
        ..ProptestConfig::default()
    })]

    /// A run is the same every time: the same events make the same trace and leave the same
    /// drive, whatever the setup (the ids are counted, and nothing hangs on the order a hash
    /// map holds things in), so a saved seed replays the run it was saved for.
    #[test]
    fn a_run_replays_the_same(
        setup in 0..SETUPS.len(),
        events in proptest::collection::vec(any_event(5), 0..120),
    ) {
        let replay = || {
            let (prepare, guard) = SETUPS[setup];
            let mut sim = prepare(Sim::new(3));
            sim.guard = guard;
            sim.verbose = true;
            sim.run(&events);
            let settled = sim.quiesce(30);
            let paths = sim.server_paths();
            (settled, sim.trace, paths)
        };
        let ((settled, trace, paths), (again, retrace, repaths)) = (replay(), replay());
        // Where the two runs part, not both traces whole.
        if let Some(at) = (0..trace.len().max(retrace.len())).find(|i| trace.get(*i) != retrace.get(*i)) {
            prop_assert!(false, "the runs part at trace line {at}:\n{:?}\n{:?}", trace.get(at), retrace.get(at));
        }
        prop_assert_eq!((settled, paths), (again, repaths));
    }
}
