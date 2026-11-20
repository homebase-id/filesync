//! Randomized runs of an owner and a member of what it shares (sync semantics §13): the
//! member's read-only folder is what is shared with it, an item whose folder isn't at the top.

use std::collections::BTreeMap;

use hbsync_remote::RemoteHeader;
use hbsync_remote::contract::Shares;

use super::*;

/// A top-level item of the member's folder with all in it: each path, and a file's bytes.
type Item = Vec<(String, Vec<u8>)>;

/// What the member's folder holds when it has all that is shared with it, by the name key of
/// each top-level item: the item, or each of the items that meet at the top under one name
/// (each shared without its folder), where the member has one and tells the clash (§13).
///
/// A node is shared when its share is live. It is in its folder when that is shared and
/// live, out of sight when that is shared and in Recently deleted (taken along), and at the
/// top otherwise.
fn shared_tree(server: &FakeServer) -> Res<BTreeMap<String, Vec<Item>>> {
    let shared = |h: &&RemoteHeader| server.shared_live(h.file_id);
    let live: HashMap<NodeId, &RemoteHeader> = server
        .live()
        .filter(shared)
        .filter_map(|h| Some((h.node()?, h)))
        .collect();
    let removed: HashSet<NodeId> = server
        .removed()
        .filter(shared)
        .filter_map(|h| h.node())
        .collect();
    // The top-level item `h` is in (the last of its folders the picture has) and the names
    // from it down to `h`, or none when a folder it is in is in Recently deleted.
    fn path<'a>(
        mut h: &'a RemoteHeader,
        live: &HashMap<NodeId, &'a RemoteHeader>,
        removed: &HashSet<NodeId>,
    ) -> Option<(NodeId, Vec<String>)> {
        let mut path = Vec::new();
        loop {
            path.push(h.state()?.name);
            match live.get(&h.parent) {
                _ if removed.contains(&h.parent) => return None,
                Some(parent) if path.len() < 64 => h = parent,
                _ => break,
            }
        }
        path.reverse();
        Some((h.node()?, path))
    }
    let mut tree: BTreeMap<String, BTreeMap<NodeId, Item>> = BTreeMap::new();
    for h in live.values() {
        let Some((top, path)) = path(h, &live, &removed) else {
            continue;
        };
        let entry = match h.kind {
            hbsync_types::NodeKind::Folder => (format!("{}/", path.join("/")), Vec::new()),
            hbsync_types::NodeKind::File => (path.join("/"), server.payload(h.file_id)?.to_vec()),
        };
        tree.entry(name_key(&path[0]))
            .or_default()
            .entry(top)
            .or_default()
            .push(entry);
    }
    Ok(tree
        .into_iter()
        .map(|(key, items)| {
            let items = items.into_values().map(|mut item| {
                item.sort();
                item
            });
            (key, items.collect())
        })
        .collect())
}

/// The member's folder, by the name key of each top-level item.
fn folder_tree(c: &Client) -> BTreeMap<String, Item> {
    let mut tree: BTreeMap<String, Item> = BTreeMap::new();
    for (path, bytes) in c.entries("") {
        let top = path.split('/').next().unwrap_or_default();
        tree.entry(name_key(top)).or_default().push((path, bytes));
    }
    tree.values_mut().for_each(|item| item.sort());
    tree
}

/// The owner works in its folder and shares, withdraws and shares again what is on its
/// drive, folders and files alike, with or without what they are in; the member syncs now
/// and then. Whenever it has, its folder is what is shared with it, and nothing was moved
/// out of it. Where items meet at the top under one name the member tells so and keeps what
/// it has of each (§13: the one synced keeps the name, the others wait as they are, which
/// this test doesn't follow): its folder is compared again once the clash is over. While one
/// lasts, an item whose name is held by one that waits on the clash may be tried again, told
/// as failed ([`Behind::Allowed`]: a known limit, filesync #400).
#[test]
fn randomized_shares_show_what_is_shared() -> R {
    compared_mostly(Besides::Nothing)
}

/// Every seed of [`shares_of`] with `besides`, the member's folder compared after most of
/// its syncs.
fn compared_mostly(besides: Besides) -> R {
    let compared = each_seed(seeds(), |seed| shares_of(seed, Behind::Allowed, besides))?;
    let (compared, synced) = compared
        .iter()
        .fold((0, 0), |sum, c| (sum.0 + c.0, sum.1 + c.1));
    eprintln!("milestone: the member's folder compared after {compared} of {synced} syncs");
    assert!(compared * 2 > synced, "{compared} of {synced}");
    Ok(())
}

/// [`randomized_shares_show_what_is_shared`], with shares given until a time and the time
/// passing: the drive writes nothing as a share ends, and the member's folder still shows
/// just what is shared with it at its every sync (sync semantics §13, "A share that ends").
#[test]
fn randomized_shares_that_end_show_what_is_shared() -> R {
    compared_mostly(Besides::Ends)
}

/// [`randomized_shares_show_what_is_shared`], with items of the drive deleted for good behind
/// everyone's back (another app's delete, the sharer's purge): the drive tells the member
/// nothing of them, and a week on, having read the share whole again, its folder shows just
/// what is shared with it (sync semantics §13, "Deleted for good by the sharer").
#[test]
fn randomized_shares_deleted_for_good_show_what_is_shared() -> R {
    compared_mostly(Besides::GoneForGood)
}

/// Seeds of [`randomized_shares_show_what_is_shared`] that failed:
/// - 194: a shared item's folder came into the picture in Recently deleted, and the item,
///   read as at the top until then, wasn't planned again: it stayed in the member's folder.
/// - 5403755731358821085: a file whose folder was no longer shared took the folder's name
///   and was edited. Its download to the top waited for the name, and the folder's delete
///   for the file in it, sync after sync (the executor: the same in any folder).
#[test]
fn regression_shares_seeds() -> R {
    each_seed([194, 5_403_755_731_358_821_085], |seed| {
        shares_of(seed, Behind::Refused, Besides::Nothing)
    })
    .map(drop)
}

/// Seed 7425484525444276840, as it should go: no operation fails. Today one does (filesync
/// #400). A file `b` is at the top of the share and its folder `b` is shared again: a clash,
/// told and waited out. The folder `c.txt`, which belongs in `b`, stays at the top meanwhile,
/// so the download of a new shared file `c.txt` finds its name taken and is tried again, sync
/// after sync, told as failed where it waits on that clash. Nothing is lost (its bytes are
/// kept for the retry), and it settles when the sharer ends the clash.
#[test]
#[ignore = "known limit: a download behind a name clash at a share's top shows as failed (#400)"]
fn known_limit_a_download_behind_a_clash_shows_as_failed() -> R {
    each_seed([7_425_484_525_444_276_840], |seed| {
        shares_of(seed, Behind::Refused, Besides::Nothing)
    })
    .map(drop)
}

/// What happens in [`shares_of`] besides shares given and withdrawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Besides {
    Nothing,
    /// Some shares are given an end, and the time passes that ends them.
    Ends,
    /// Items of the drive are deleted for good, and a week passes before the member syncs.
    GoneForGood,
}

/// Whether [`shares_of`] takes an operation tried again for a taken name while a clash at the
/// top is told (filesync #400), or fails on it as on any failed operation.
#[derive(Clone, Copy, PartialEq)]
enum Behind {
    Allowed,
    Refused,
}

/// One seed of [`randomized_shares_show_what_is_shared`]: after how many of the member's
/// syncs its folder was compared, and how many it had.
fn shares_of(seed: u64, behind_a_clash: Behind, besides: Besides) -> Res<(usize, usize)> {
    const MINUTE: i64 = 60_000;
    const WEEK: i64 = 7 * 24 * 60 * MINUTE;
    // (With nothing besides, the seeds draw as they did before: the saved ones hold.)
    let ending = besides == Besides::Ends;
    // Something went for good that the member hasn't read the share whole for since.
    let mut unheard = false;
    let mut compared = (0, 0);
    let server = server();
    let mut a = Client::with_config(&server, "a", |c| c.guard.allow = true)?;
    let (mut b, out) = Client::member(&server, "b")?;
    let mut rng = Rng(seed);
    let mut ledger = Ledger::default();
    for step in 0..40 {
        match rng.below(5 + usize::from(besides != Besides::Nothing)) {
            0 | 1 => act(&a.root, &mut rng, &mut ledger)?,
            // An item of the drive, live or in Recently deleted, is deleted for good: no row
            // in the feed, nothing announced.
            5 if besides == Besides::GoneForGood => {
                let mut s = server.lock().map_err(|_| "poisoned")?;
                let mut on_drive: Vec<_> = (s.live().chain(s.removed()))
                    .filter_map(|h| Some((h.state()?.name, h.changed_at, h.file_id, h.version)))
                    .collect();
                on_drive.sort();
                if on_drive.is_empty() {
                    continue;
                }
                let (.., file_id, version) = on_drive[rng.below(on_drive.len())];
                s.hard_delete(file_id, version)?;
                unheard = true;
            }
            // Time passes on the server: nothing is written, and shares given until then end.
            5 if ending => {
                let minutes = 1 + rng.below(3) as i64;
                server
                    .lock()
                    .map_err(|_| "poisoned")?
                    .advance(minutes * MINUTE);
            }
            2 => {
                a.sync()?;
            }
            3 => {
                a.sync()?;
                let mut on_drive: Vec<_> = {
                    let s = server.lock().map_err(|_| "poisoned")?;
                    let all = s.live().chain(s.removed());
                    all.filter_map(|h| {
                        let name = h.state()?.name;
                        Some((name, h.changed_at, h.file_id, s.shared_live(h.file_id)))
                    })
                    .collect()
                };
                on_drive.sort();
                if on_drive.is_empty() {
                    continue;
                }
                let (.., file_id, shared) = on_drive[rng.below(on_drive.len())];
                match shared && rng.below(2) == 0 {
                    true => a.remote.withdraw(file_id)?,
                    // Until a whole minute a few on (the member asks by the minute), whether
                    // it is shared now or not: an end given, changed, or given anew.
                    false if ending && rng.below(2) == 0 => {
                        let now = server.lock().map_err(|_| "poisoned")?.now();
                        let minutes = now.div_euclid(MINUTE) + 1 + rng.below(3) as i64;
                        a.remote.share_until(file_id, minutes * MINUTE)?;
                    }
                    false => a.remote.share(file_id)?,
                }
                server.lock().map_err(|_| "poisoned")?.tick();
            }
            _ => {
                // A week on the member reads the share whole again, and asks about what the
                // feed no longer shows: not there, said twice a minute apart (§8b). So it
                // syncs twice then, each sync held to the same, its folder compared after
                // the second.
                let a_week_on = std::mem::take(&mut unheard);
                if a_week_on {
                    (server.lock().map_err(|_| "poisoned")?).advance(WEEK + MINUTE);
                }
                let syncs: &[bool] = if a_week_on { &[true, false] } else { &[false] };
                let mut told = None;
                for &asks in syncs {
                    let due = b.engine.db().read(|s| s.reread_due())?;
                    let report = b.sync()?;
                    assert!(out.path().read_dir()?.next().is_none(), "moved out");
                    if asks {
                        // Told "not there" once, the member still has what went for good,
                        // which may hold a name at the top: nothing else may fail.
                        let waits = behind_a_clash == Behind::Allowed;
                        assert!(
                            (report.failed.iter()).all(|(_, why)| waits && why == "name taken"),
                            "step {step}, a week on: {report:?}"
                        );
                        b.a_minute_on()?;
                        continue;
                    }
                    let shared = shared_tree(&*server.lock().map_err(|_| "poisoned")?)?;
                    let clash = shared.values().any(|items| items.len() > 1);
                    let waits = clash && behind_a_clash == Behind::Allowed;
                    assert!(
                        (report.failed.iter()).all(|(_, why)| waits && why == "name taken"),
                        "step {step}: {report:?}"
                    );
                    assert_eq!(
                        !report.conflicts.is_empty(),
                        clash,
                        "step {step}: {report:?}"
                    );
                    // A time to read the share from its beginning by is kept, and only a
                    // week gone by (or an end) has it read so: no other sync moves the time.
                    let after = b.engine.db().read(|s| s.reread_due())?;
                    assert!(after.is_some(), "step {step}: no time kept");
                    if !ending && due.is_some() {
                        assert_eq!(after, due, "step {step}: read whole");
                    }
                    told = Some((shared, clash));
                }
                let (shared, clash) = told.ok_or("no sync")?;
                compared.1 += 1;
                if clash {
                    continue;
                }
                compared.0 += 1;
                let shared: BTreeMap<String, Item> = shared
                    .into_iter()
                    .filter_map(|(key, mut items)| Some((key, items.pop()?)))
                    .collect();
                assert_eq!(folder_tree(&b), shared, "step {step}");
            }
        }
    }
    Ok(compared)
}
