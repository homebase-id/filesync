//! The identity's "not there" (sync semantics §8b): with [`Sim::with_wrong_not_theres`] the
//! drive answers one wrong 404 now and then, for a live file a client holds. A client judges
//! each "not there" as the engine does ([`hbsync_engine::gone::judge`]): once is a suspicion,
//! asked again at its next sync (a minute on, by its clock); the drive answers that truly. A
//! file judged gone is gone for that client for good (`gone_files`, by file id: a node created
//! again is another file): its plans no longer see it. [`Sim::check`] fails if a client judged
//! a file gone that the drive still holds.
//!
//! [`Sim::with_wrong_not_theres`]: crate::Sim::with_wrong_not_theres
//! [`Sim::check`]: crate::Sim::check

use hbsync_engine::gone::{CONFIRM_AFTER_MS, NotThere, judge};
use hbsync_engine::tree::Tree;
use hbsync_state::GoneSuspect;
use hbsync_types::{NodeId, NodeKind, RemoteFileId, VersionTag};

use crate::Sim;

/// Every this many plans, the drive answers a wrong "not there".
const LIE_EVERY: u64 = 3;

/// A file: its node and its file id.
pub(crate) type File = (NodeId, RemoteFileId);

impl Sim {
    /// Client `c` syncs: it asks again about what it suspects (the drive answers truly), and,
    /// now and then, the drive says a live file it holds isn't there.
    pub(crate) fn hear_not_theres(&mut self, c: usize) {
        if !self.lying {
            return;
        }
        let now = {
            let client = &mut self.clients[c];
            client.syncs += 1;
            client.syncs * CONFIRM_AFTER_MS
        };
        let suspects: Vec<_> = self.clients[c].suspects.clone().into_iter().collect();
        for (file, prior) in suspects {
            if self.holds(file) {
                // There: the suspicion goes.
                self.clients[c].suspects.remove(&file);
            } else {
                self.not_there(c, file, prior.version, now);
            }
        }
        self.plans_heard += 1;
        if !self.plans_heard.is_multiple_of(LIE_EVERY) {
            return;
        }
        // A live file the client holds, said of once only (picked in id order: the base's
        // order is a hash map's).
        let mut held: Vec<NodeId> = self.clients[c]
            .base
            .iter()
            .filter(|(n, s)| {
                s.kind == NodeKind::File
                    && self.server.live.contains_key(n)
                    && !self.lied.contains(n)
            })
            .map(|(n, _)| n)
            .collect();
        held.sort();
        if held.is_empty() {
            return;
        }
        let node = held[(self.plans_heard / LIE_EVERY) as usize % held.len()];
        self.lied.insert(node);
        let at = self.server.live[&node].remote;
        self.note(|_| format!("c{c}: the drive says {node:?} isn't there (wrongly)"));
        self.not_there(c, (node, at.file_id), Some(at.version), now);
    }

    /// Whether the drive holds `file`, live or in Recently deleted.
    fn holds(&self, (node, file): File) -> bool {
        [&self.server.live, &self.server.removed]
            .iter()
            .any(|m| m.get(&node).is_some_and(|n| n.remote.file_id == file))
    }

    /// Client `c` hears the identity's "not there" of `file`, known at `version`, at `now`.
    fn not_there(&mut self, c: usize, file: File, version: Option<VersionTag>, now: i64) {
        let prior: Option<GoneSuspect> = self.clients[c].suspects.get(&file).copied();
        match judge(prior, version, now) {
            NotThere::Gone => {
                self.clients[c].suspects.remove(&file);
                self.clients[c].gone.insert(file.1);
                if self.holds(file) && self.gone_while_there.is_none() {
                    self.gone_while_there = Some(format!(
                        "c{c} took {:?} as gone for good, which the drive holds",
                        file.0
                    ));
                }
            }
            NotThere::Suspect(s) => {
                self.clients[c].suspects.insert(file, s);
            }
        }
    }

    /// Whether a client took a file the drive holds as gone for good ([`Sim::check`] says
    /// which).
    pub fn took_a_live_file_as_gone(&self) -> bool {
        self.gone_while_there.is_some()
    }

    /// The drive as client `c`'s plan sees it: without what it took as gone for good.
    pub(crate) fn drive_seen(&self, c: usize) -> Tree {
        let mut view = self.server_view(c);
        let gone = &self.clients[c].gone;
        for (n, s) in &self.server.live {
            if gone.contains(&s.remote.file_id) {
                view.remove(*n);
            }
        }
        view
    }
}
