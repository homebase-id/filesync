//! A finished sync as people see it: its conflicts and failures worded, the held deletes, and
//! the folder's status after it.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use hbsync_engine::exec::Skip;
use hbsync_engine::{Conflict, Engine, Op, SyncReport};
use hbsync_state::StateError;
use hbsync_types::NodeId;

use crate::protocol::{FolderStatus, HeldDeletes, LastSync, Note, State};

/// Entries kept in [`FolderStatus::recent`].
const RECENT: usize = 50;
/// Paths shown for held deletes.
const EXAMPLES: usize = 10;

/// A stale skip is told when it keeps coming back this long: something holds it back that the
/// plan can't see (a file written all the time, one this device can't read).
const STALLED_FOR: Duration = Duration::from_secs(600);
/// ...and at least this often within that time.
const STALLED_SKIPS: u32 = 3;

/// Each conflict, failure and told skip of `report` (`skips`, [`Stalled::told`]), and the
/// drive's rows the client couldn't read (`rows`), worded for people.
pub(super) fn notes(
    engine: &mut Engine,
    report: &SyncReport,
    skips: &[&(Op, Skip)],
    rows: Vec<String>,
) -> Result<Vec<String>, StateError> {
    let mut notes = Vec::new();
    for c in &report.conflicts {
        notes.push(describe_conflict(engine, c)?);
    }
    let failed = report.failed.iter().map(|(op, why)| (op, why.as_str()));
    for (op, why) in failed.chain(skips.iter().map(|(op, why)| (op, why.as_str()))) {
        let path = engine.path_of(op.node())?.unwrap_or_else(|| "?".into());
        notes.push(format!("{path}: {} ({why})", op.kind()));
    }
    if report.full.is_some() {
        notes.push(
            "Homebase is out of storage: new and changed files wait until there is room".into(),
        );
    }
    // Drive rows the client can't read (not encrypted, another key, not a sync node).
    for why in rows {
        notes.push(format!("a file on Homebase can't be synced: {why}"));
    }
    Ok(notes)
}

/// Notes told once: one that keeps coming back sync after sync (a file that can't be read)
/// isn't told again until it has gone away; one that comes twice in a sync (a node planned
/// again in a later sweep), once.
#[derive(Debug, Default)]
pub(super) struct Told(HashSet<String>);

impl Told {
    /// The notes of the last sync that weren't in the one before, each once.
    pub fn fresh(&mut self, mut notes: Vec<String>) -> Vec<String> {
        let before = std::mem::take(&mut self.0);
        notes.retain(|n| self.0.insert(n.clone()) && !before.contains(n));
        notes
    }
}

/// Stale skips sync after sync: per operation (node and kind), when it was first skipped and
/// how often since, until it isn't skipped any more.
#[derive(Debug, Default)]
pub(super) struct Stalled(HashMap<(NodeId, &'static str), Streak>);

#[derive(Debug, Clone, Copy)]
struct Streak {
    first: Instant,
    last: Instant,
    skips: u32,
    told: bool,
}

impl Stalled {
    /// The skips of a sync ended at `now` to tell, each operation once: those this device can't
    /// do, and stale ones skipped [`STALLED_SKIPS`] times over [`STALLED_FOR`] (once a streak).
    /// One waiting for another operation isn't told: that one says why.
    pub fn told<'r>(&mut self, skipped: &'r [(Op, Skip)], now: Instant) -> Vec<&'r (Op, Skip)> {
        let mut seen = HashSet::new();
        let mut told = Vec::new();
        for item in skipped {
            let (op, why) = item;
            let key = (op.node(), op.kind());
            if !seen.insert(key) {
                continue;
            }
            let tell = match why {
                Skip::Unsyncable(_) => true,
                Skip::Waiting(_) => false,
                Skip::Stale(_) => {
                    let streak = self.0.entry(key).or_insert(Streak {
                        first: now,
                        last: now,
                        skips: 0,
                        told: false,
                    });
                    streak.last = now;
                    streak.skips += 1;
                    let stalled =
                        streak.skips >= STALLED_SKIPS && now >= streak.first + STALLED_FOR;
                    stalled && !std::mem::replace(&mut streak.told, stalled)
                }
            };
            if tell {
                told.push(item);
            }
        }
        // A streak ends once the operation wasn't skipped for a while (settled, or not planned).
        self.0.retain(|_, s| now < s.last + STALLED_FOR);
        told
    }
}

/// The deletes the guard held in `report` (of `nodes`), as the status shows them; `None` when
/// none were.
pub(super) fn held(
    engine: &mut Engine,
    report: &SyncReport,
    nodes: &[NodeId],
) -> Result<Option<HeldDeletes>, StateError> {
    if report.held.is_empty() {
        return Ok(None);
    }
    let here = report
        .held
        .iter()
        .filter(|op| matches!(op, Op::RemoteDelete { .. }))
        .count() as u64;
    let mut examples = Vec::new();
    for op in report.held.iter().take(EXAMPLES) {
        examples.extend(engine.path_of(op.node())?);
    }
    Ok(Some(HeldDeletes {
        here,
        remotely: report.held.len() as u64 - here,
        examples,
        token: held_token(nodes),
    }))
}

/// The token of the held deletes of `nodes`, a hash of their sorted ids (made and checked by
/// one daemon process).
pub(super) fn held_token(nodes: &[NodeId]) -> String {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut sorted = nodes.to_vec();
    sorted.sort();
    let mut hasher = DefaultHasher::new();
    sorted.hash(&mut hasher);
    format!("{:016x}-{}", hasher.finish(), sorted.len())
}

/// The folder's status after a sync that ended at `at` (Unix seconds) and took `took`.
pub(super) fn record(
    s: &mut FolderStatus,
    report: &SyncReport,
    notes: Vec<String>,
    held: Option<HeldDeletes>,
    took: Duration,
    at: u64,
) {
    s.state = State::Idle;
    s.error = None;
    s.retry_at = None;
    s.pending = report.pending;
    s.held_deletes = held;
    s.last_sync = Some(LastSync {
        at,
        millis: took.as_millis() as u64,
        done: report.ops_done,
        downloaded_changes: report.feed.live + report.feed.deleted,
        adopted: report.adopted,
    });
    s.notes_added += notes.len() as u64;
    s.recent
        .extend(notes.into_iter().map(|text| Note { at, text }));
    let excess = s.recent.len().saturating_sub(RECENT);
    s.recent.drain(..excess);
    s.synced_added += report.synced.len() as u64;
}

fn describe_conflict(engine: &mut Engine, c: &Conflict) -> Result<String, StateError> {
    let mut path = |node| -> Result<String, StateError> {
        Ok(engine.path_of(node)?.unwrap_or_else(|| "?".into()))
    };
    Ok(match c {
        Conflict::EditEdit { node, copy } => format!(
            "{}: edited on two devices; this device's version is kept as {}",
            path(*node)?,
            path(*copy)?
        ),
        Conflict::MoveMove { node } => {
            format!("{}: moved on two devices; the other move won", path(*node)?)
        }
        Conflict::EditDelete { node } => format!(
            "{}: deleted on one device and edited on another; the edit was kept",
            path(*node)?
        ),
        Conflict::NameClash { node } => {
            format!("{}: renamed, as the name was taken", path(*node)?)
        }
        Conflict::Cycle { node } => format!(
            "{}: a move that would have put a folder inside itself was undone",
            path(*node)?
        ),
        Conflict::Orphan { node } => format!(
            "{}: its folder was deleted everywhere; it was moved to the top folder",
            path(*node)?
        ),
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn a_note_is_told_once_until_it_goes_away() {
        let mut told = Told::default();
        let note = |s: &str| vec![s.to_owned()];
        assert_eq!(
            told.fresh(note("a.txt: can't read")),
            note("a.txt: can't read")
        );
        assert!(told.fresh(note("a.txt: can't read")).is_empty());
        assert!(told.fresh(Vec::new()).is_empty());
        assert_eq!(
            told.fresh(note("a.txt: can't read")),
            note("a.txt: can't read")
        );
    }

    #[test]
    fn the_status_keeps_the_newest_notes_and_counts_them_all() {
        let mut s = FolderStatus::new("a.example", Path::new("/home/a/Homebase"));
        s.state = State::Syncing;
        s.error = Some("earlier".into());
        let report = SyncReport {
            ops_done: 3,
            pending: 2,
            ..SyncReport::default()
        };
        let notes: Vec<String> = (0..60).map(|i| format!("note {i}")).collect();
        record(
            &mut s,
            &report,
            notes,
            None,
            Duration::from_millis(1500),
            1_800_000_000,
        );
        assert_eq!((s.state, s.error, s.pending), (State::Idle, None, 2));
        assert_eq!(s.notes_added, 60);
        assert_eq!(s.recent.len(), RECENT);
        assert_eq!(s.recent.last().map(|n| n.text.as_str()), Some("note 59"));
        let last = s.last_sync.ok_or("no last sync");
        assert_eq!(last.map(|l| (l.done, l.millis)), Ok((3, 1500)));
    }

    #[test]
    fn stale_skips_are_told_only_when_they_keep_coming_back() {
        let (stuck, waiting, bad) = (NodeId::new(), NodeId::new(), NodeId::new());
        let settle = |node| Op::Settle { node };
        let sync = vec![
            (settle(stuck), Skip::Stale("changed on disk".into())),
            (settle(stuck), Skip::Stale("changed on disk".into())),
            (settle(waiting), Skip::Waiting("its children first".into())),
            (
                settle(bad),
                Skip::Unsyncable("name can't be used here".into()),
            ),
        ];
        let nodes =
            |told: Vec<&(Op, Skip)>| told.iter().map(|(op, _)| op.node()).collect::<Vec<_>>();
        let mut stalled = Stalled::default();
        let t0 = Instant::now();
        let minute = Duration::from_secs(60);
        assert_eq!(
            nodes(stalled.told(&sync, t0)),
            [bad],
            "only what can't be done, once"
        );
        // Skipped again and again within seconds (a file written all the time): not yet.
        for i in 1..20 {
            assert_eq!(
                nodes(stalled.told(&sync[..1], t0 + Duration::from_secs(i))),
                Vec::<NodeId>::new()
            );
        }
        // Still skipped after ten minutes: told, once.
        assert_eq!(nodes(stalled.told(&sync[..1], t0 + 10 * minute)), [stuck]);
        assert_eq!(
            nodes(stalled.told(&sync[..1], t0 + 11 * minute)),
            Vec::<NodeId>::new()
        );
        // Not skipped for ten minutes: the streak ended, a new one starts.
        assert!(stalled.told(&[], t0 + 22 * minute).is_empty());
        assert_eq!(
            nodes(stalled.told(&sync[..1], t0 + 23 * minute)),
            Vec::<NodeId>::new()
        );
    }

    #[test]
    fn a_token_names_exactly_its_deletes() {
        let (a, b) = (NodeId::new(), NodeId::new());
        assert_eq!(held_token(&[a, b]), held_token(&[b, a]));
        assert_ne!(held_token(&[a, b]), held_token(&[a]));
    }
}
