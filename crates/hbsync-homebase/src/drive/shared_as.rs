//! How this identity's own folders are shared, as far as this computer knows: what a file or
//! folder it adds to one is shared as (`docs/spec/homebase-api-notes.md`, "Sharing what this
//! identity's computer adds").
//!
//! A share is a row on each file, not on its folder: a new file reaches a folder's members
//! only with rows of its own. The app that shares writes them for what is there
//! (**[verified]** chat-kmp `FilesyncSharing.share`), and whoever adds a file writes the new
//! one's: this client with the upload itself, as the folder it goes into is shared.
//!
//! What was read of a folder is gone by only while a change of it would have been told:
//! by the notifications for as long as their connection holds ([`Folders::heard`]), else
//! for a few minutes, in which the feed tells of it ([`Folders::fed`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

use hbsync_types::{NodeId, RemoteFileId, RemoteRef};
use uuid::Uuid;

/// How long what was learnt of a folder's shares is gone by where the notifications' one
/// connection didn't hold all the while since ([`Folders::known`]): the feed, read every few
/// minutes while they are down and as they connect, tells of a change ([`Folders::fed`]);
/// this bounds the time before it does, at one small read per folder added to after it.
const KEPT: Duration = Duration::from_secs(600);

/// How long a folder whose shares the server refused with an upload is added to without
/// them ([`Folders::refused`]), unless it changes before: then they are read and tried again.
const REFUSED: Duration = Duration::from_secs(3600);

/// The wall clock this far ahead of the steady one since something was learnt: this
/// computer slept meanwhile (the steady clock doesn't count a sleep on Linux and macOS),
/// and the notifications' connection it was learnt under heard nothing in that time.
const SLEPT: Duration = Duration::from_secs(30);

/// More folders than this known (about 180 bytes each): the ones no longer gone by are
/// dropped as the next is learnt, and where that leaves three quarters or more, the quarter
/// asked about longest ago (a folder dropped is read again, if it is added to again).
const MANY: usize = 65_536;

/// As many changes told while one read is on its way are kept to check it against; more,
/// and the read isn't kept.
const TOLD: usize = 256;

/// A share that ends within this isn't given to a new file: the server refuses a share whose
/// end has passed, and with it the upload carrying it (**[verified]** odin-core
/// `DriveShareService.AssertShape`), and this computer's clock isn't the server's.
const ENDING: i64 = 60_000;

/// An upload is taken to send at least this many bytes a millisecond (4 Mbit/s): the server
/// looks at a file's shares once its bytes are in, so a share that ends before a file of
/// that size is up isn't given to it (its upload would be refused, sent in vain).
const SENT_A_MS: i64 = 500;

/// One live share of a folder: the circle it is shared with, what the circle may do with it
/// ([`crate::app`]'s `READ`, `WRITE`, `REACT`, `COMMENT`), and when it ends (Unix ms).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Share {
    pub circle: Uuid,
    pub permissions: u32,
    pub expires_at: Option<i64>,
}

impl Share {
    /// Still to be given to a file of `size` bytes added at `now` (Unix ms, this computer's
    /// clock): it lasts past the upload ([`ENDING`], [`SENT_A_MS`]).
    pub(crate) fn lasts(&self, now: i64, size: u64) -> bool {
        let sending = i64::try_from(size).unwrap_or(i64::MAX) / SENT_A_MS;
        let up = now.saturating_add(ENDING).saturating_add(sending);
        self.expires_at.is_none_or(|end| end > up)
    }
}

/// A folder's live shares; empty where it isn't shared.
pub(crate) type Shares = Arc<[Share]>;

/// The ones of `shares` still to be given at `now` to a node that is on the drive
/// ([`Share::lasts`]).
pub(crate) fn lasting(shares: &[Share], now: i64) -> Vec<Share> {
    (shares.iter().filter(|share| share.lasts(now, 0)))
        .copied()
        .collect()
}

/// When something was learnt, by both clocks: the steady one doesn't count a sleep, the
/// wall's can be set.
#[derive(Debug, Clone, Copy)]
struct At(Instant, SystemTime);

impl At {
    fn now() -> At {
        At(Instant::now(), SystemTime::now())
    }

    /// Less than `limit` ago, by both clocks.
    fn within(&self, limit: Duration) -> bool {
        self.0.elapsed() < limit && self.1.elapsed().is_ok_and(|by_wall| by_wall < limit)
    }

    /// This computer slept since ([`SLEPT`]), or its wall clock was set.
    fn slept(&self) -> bool {
        let by_wall = self.1.elapsed().unwrap_or(Duration::MAX);
        by_wall > self.0.elapsed().saturating_add(SLEPT)
    }
}

/// Whether two folders are shared alike: with the same circles, each for the same and until
/// the same, in whatever order (the server lists a file's shares by when each was first
/// written: **[verified]** odin-core `TableDriveAclIndex.GetRowsAsync`).
pub(crate) fn alike(a: &[Share], b: &[Share]) -> bool {
    a.len() == b.len() && a.iter().all(|share| b.contains(share))
}

/// What is known of one folder.
#[derive(Debug)]
struct Known {
    /// The folder's file, which the notifications name it by. None for a folder whose row
    /// didn't read or isn't there ([`Folders::read`]): it shares nothing, as far as is known.
    file_id: Option<Uuid>,
    /// The folder's version as its shares were read, or as this computer made it.
    version: Uuid,
    shares: Shares,
    learnt: At,
    /// The notifications' connection that was up as it was learnt: while that one holds, a
    /// change of the folder would have been heard. None for a folder they can't name.
    listening: Option<u64>,
    /// When it was last asked about ([`State::asked`]).
    asked: u64,
}

/// The one read of a folder's shares on its way ([`Folders::reading`]), and what was told
/// of the drive meanwhile: a read that a change overtook isn't kept, as what it read may be
/// from before the change.
#[derive(Debug, Default)]
struct Reading {
    /// The notifications' connection as it began.
    listening: Option<u64>,
    /// The files the notifications named, and the folders the feed had rows of.
    heard: Vec<Uuid>,
    fed: Vec<NodeId>,
    /// The notifications connected meanwhile, or more was told than is kept ([`TOLD`]).
    overtaken: bool,
}

impl Reading {
    /// One more told, of `told` so far: past [`TOLD`] the read is overtaken, and no more
    /// are kept.
    fn told<T>(&mut self, told: fn(&mut Reading) -> &mut Vec<T>, one: T) {
        match told(self).len() < TOLD {
            true => told(self).push(one),
            false => self.overtaken = true,
        }
    }
}

#[derive(Debug, Default)]
struct State {
    folders: HashMap<NodeId, Known>,
    /// The known folders by their files ([`Known::file_id`]).
    by_file: HashMap<Uuid, NodeId>,
    /// The notifications' connections so far, and the one that is up now
    /// ([`Folders::listening`]).
    connects: u64,
    listening: Option<u64>,
    reading: Option<Reading>,
    /// Counts the folders learnt and asked about ([`Known::asked`]).
    asked: u64,
    /// The folders whose shares the server refused with an upload, when, and the shares it
    /// refused ([`Folders::refused`]): kept apart from what is known of them, which is
    /// read again in between.
    refusals: HashMap<NodeId, (At, Shares)>,
}

impl State {
    /// The notifications' connection that is up heard every change of the folder since
    /// `known` was learnt: the one it was learnt under, and no sleep since.
    fn heard_all(&self, known: &Known) -> bool {
        known.listening.is_some() && known.listening == self.listening && !known.learnt.slept()
    }

    /// Whether what is `known` of a folder is still gone by ([`Folders::known`]).
    fn holds(&self, known: &Known) -> bool {
        self.heard_all(known) || known.learnt.within(KEPT)
    }

    /// `folder` changed, as far as was told: nothing is known of it any more, nor held
    /// against its shares.
    fn changed(&mut self, folder: NodeId) {
        self.forget(folder);
        self.refusals.remove(&folder);
    }

    /// Nothing is known of `folder` any more.
    fn forget(&mut self, folder: NodeId) {
        if let Some(file) = self.folders.remove(&folder).and_then(|known| known.file_id) {
            self.by_file.remove(&file);
        }
    }

    /// `folder` is shared as `shares`: the file `at`, or (`None`) one the notifications
    /// can't name, which their connection doesn't vouch for.
    fn learn(&mut self, folder: NodeId, at: Option<RemoteRef>, shares: Shares) {
        if self.folders.len() >= MANY {
            self.make_room();
        }
        self.forget(folder);
        self.asked += 1;
        let known = Known {
            file_id: at.map(|at| at.file_id.0),
            version: at.map(|at| at.version.0).unwrap_or_default(),
            shares,
            learnt: At::now(),
            listening: self.listening.filter(|_| at.is_some()),
            asked: self.asked,
        };
        self.folders.insert(folder, known);
        if let Some(at) = at {
            self.by_file.insert(at.file_id.0, folder);
        }
    }
}

impl State {
    /// Drops the folders no longer gone by; and where that leaves three quarters of
    /// [`MANY`] or more (a tree of that many folders made in one go, all still vouched
    /// for), the quarter asked about longest ago: a scan of all, once per quarter learnt.
    fn make_room(&mut self) {
        let mut folders = std::mem::take(&mut self.folders);
        folders.retain(|_, known| self.holds(known));
        if folders.len() >= MANY / 4 * 3 {
            let mut asked: Vec<u64> = folders.values().map(|known| known.asked).collect();
            let (_, oldest_kept, _) = asked.select_nth_unstable(MANY / 4);
            let oldest_kept = *oldest_kept;
            folders.retain(|_, known| known.asked >= oldest_kept);
        }
        self.by_file
            .retain(|_, folder| folders.contains_key(folder));
        self.folders = folders;
    }
}

/// The folders of this identity's drive whose shares this computer knows.
#[derive(Debug, Default)]
pub(crate) struct Folders(Mutex<State>);

/// A feed row, as far as a folder's shares go.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Fed {
    pub node: NodeId,
    pub version: Uuid,
    /// The row's `updated` is its `versionUpdated`: nothing changed the file since the write
    /// that gave it its version. A share given or withdrawn moves `updated` alone
    /// (**[verified]** odin-core `DriveShareService.WriteSharesAsync`, `WithdrawShareAsync`).
    pub as_written: bool,
}

impl Fed {
    /// What a folder's row says: its node, its version, when it changed last and when its
    /// version was written (Unix ms). A row that doesn't say when isn't taken as unchanged.
    pub(crate) fn of(
        node: Option<Uuid>,
        version: Uuid,
        updated: Option<i64>,
        version_updated: Option<i64>,
    ) -> Option<Fed> {
        Some(Fed {
            node: NodeId(node?),
            version,
            as_written: updated.is_some() && updated == version_updated,
        })
    }
}

impl Folders {
    fn state(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `folder`'s shares, if a change of them since they were learnt would have been told:
    /// for as long as the notifications' connection they were learnt under holds (a change
    /// of the folder is announced within seconds, [`Folders::heard`]) and this computer
    /// didn't sleep, else for [`KEPT`].
    pub(crate) fn known(&self, folder: NodeId) -> Option<Shares> {
        let mut state = self.state();
        state.asked += 1;
        let asked = state.asked;
        let holds = state.holds(state.folders.get(&folder)?);
        let known = state.folders.get_mut(&folder)?;
        known.asked = asked;
        holds.then(|| Arc::clone(&known.shares))
    }

    /// The notifications connected (`true`), or their connection broke. What was learnt
    /// under another connection is gone by for [`KEPT`] from then on: what they missed, the
    /// feed tells.
    pub(crate) fn listening(&self, connected: bool) {
        let mut state = self.state();
        state.listening = connected.then(|| {
            state.connects += 1;
            state.connects
        });
        if let (true, Some(reading)) = (connected, &mut state.reading) {
            reading.overtaken = true;
        }
    }

    /// A read of a folder's shares begins: the one on its way (who reads holds the remote's
    /// lock for it), ended by [`Folders::read`].
    pub(crate) fn reading(&self) {
        let mut state = self.state();
        state.reading = Some(Reading {
            listening: state.listening,
            ..Reading::default()
        });
    }

    /// The read that began ([`Folders::reading`]) ended: without an answer (`None`), or
    /// saying `folder` is shared as the shares given, as the file given; no file for a
    /// folder whose row isn't there or doesn't read, which is kept as sharing nothing for
    /// [`KEPT`] (not asked about with every node added to it). An answer is kept unless
    /// something was told of the folder meanwhile, or the notifications' connection isn't
    /// the one it began under.
    pub(crate) fn read(&self, folder: NodeId, answer: Option<(Option<RemoteRef>, Shares)>) {
        let mut state = self.state();
        let (Some(reading), Some((at, shares))) = (state.reading.take(), answer) else {
            return;
        };
        let overtaken = reading.overtaken
            || reading.listening != state.listening
            || at.is_some_and(|at| reading.heard.contains(&at.file_id.0))
            || reading.fed.contains(&folder);
        if !overtaken {
            state.learn(folder, at, shares);
        }
    }

    /// This computer made `folder` (the file `at`), shared as `shares`.
    pub(crate) fn made(&self, folder: NodeId, at: RemoteRef, shares: Shares) {
        self.state().learn(folder, Some(at), shares);
    }

    /// This computer wrote `folder` to the version `at`: its shares are as they were (a
    /// write keeps them), and its own write coming back in the feed is no news. Only where
    /// the notifications heard every change of it so far: else a share's change they missed
    /// would pass for this write in the feed (both move `updated`), so the folder is read
    /// again.
    pub(crate) fn rewritten(&self, folder: NodeId, at: RemoteRef) {
        let mut state = self.state();
        let heard_all =
            |known: &Known| known.file_id == Some(at.file_id.0) && state.heard_all(known);
        match state.folders.get(&folder).map(heard_all) {
            Some(true) => {
                if let Some(known) = state.folders.get_mut(&folder) {
                    known.version = at.version.0;
                }
            }
            Some(false) => state.forget(folder),
            None => {}
        }
    }

    /// The server refused an upload into `folder` that carried the shares `sent`: what is
    /// added to it goes up without them for [`REFUSED`], or until the folder changes. Only
    /// where `sent` is still how the folder is known to be shared (whether it was): one
    /// that changed meanwhile shares what it shares now, which nothing refused.
    pub(crate) fn refused(&self, folder: NodeId, sent: &Shares) -> bool {
        let mut state = self.state();
        let as_sent = |known: &Known| known.shares[..] == sent[..];
        if !state.folders.get(&folder).is_some_and(as_sent) {
            return false;
        }
        state.refusals.retain(|_, (at, _)| at.within(REFUSED));
        state.refusals.insert(folder, (At::now(), Arc::clone(sent)));
        true
    }

    /// Whether what is added to `folder`, shared as `shares`, goes up without them for now:
    /// the server refused those with an upload less than [`REFUSED`] ago
    /// ([`Folders::refused`]), and nothing was told of the folder since. However often it
    /// is read again meanwhile (with the notifications down, every [`KEPT`]).
    pub(crate) fn is_refused(&self, folder: NodeId, shares: &[Share]) -> bool {
        let refused = |(at, refused): &(At, Shares)| at.within(REFUSED) && refused[..] == *shares;
        self.state().refusals.get(&folder).is_some_and(refused)
    }

    /// Nothing is known of `folder` any more: it is read again when next added to.
    pub(crate) fn forget(&self, folder: NodeId) {
        self.state().forget(folder);
    }

    /// An upload into `folder` was refused without shares too: it wasn't for them that the
    /// one before it was ([`Folders::refused`]). The folder is read, and shared, again.
    pub(crate) fn not_for_its_shares(&self, folder: NodeId) {
        let mut state = self.state();
        if state.refusals.remove(&folder).is_some() {
            state.forget(folder);
        }
    }

    /// The notifications named `file`, or the feed has a row of it that doesn't read: if it
    /// is a known folder, it is read again when next added to. The notifications announce
    /// what others did (this computer's own writes aren't announced to it), and a share
    /// given or withdrawn is announced as a change that keeps the folder's version
    /// (**[verified]** live 2026-11-25): any word of a known folder counts.
    pub(crate) fn heard(&self, file: RemoteFileId) {
        let mut state = self.state();
        if let Some(folder) = state.by_file.get(&file.0).copied() {
            state.changed(folder);
        }
        if let Some(reading) = &mut state.reading {
            reading.told(|reading| &mut reading.heard, file.0);
        }
    }

    /// A feed row of a folder: one that isn't as it was learnt (another version, or changed
    /// since its version was written, as a share given or withdrawn changes it) is read
    /// again when next added to. This computer's own write of a folder comes back as it was
    /// written, and stays known; but for a folder it made with shares, whose share rows the
    /// server writes a millisecond after the header (**[verified]** live 2026-11-26, and
    /// odin-core `WriteUploadShares`): that one is read once when next added to after the
    /// feed was read.
    pub(crate) fn fed(&self, row: Fed) {
        let mut state = self.state();
        let same = |known: &Known| row.as_written && known.version == row.version;
        if state
            .folders
            .get(&row.node)
            .is_some_and(|known| !same(known))
        {
            state.changed(row.node);
        }
        if let Some(reading) = &mut state.reading {
            reading.told(|reading| &mut reading.fed, row.node);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn share(circle: u128, expires_at: Option<i64>) -> Share {
        Share {
            circle: Uuid::from_u128(circle),
            permissions: crate::app::READ,
            expires_at,
        }
    }

    /// The folder's file `file` at `version`.
    pub(crate) fn at(file: u128, version: u128) -> RemoteRef {
        RemoteRef {
            file_id: RemoteFileId(Uuid::from_u128(file)),
            version: hbsync_types::VersionTag(Uuid::from_u128(version)),
        }
    }

    fn fed(node: NodeId, version: u128, as_written: bool) -> Fed {
        Fed {
            node,
            version: Uuid::from_u128(version),
            as_written,
        }
    }

    fn file(file: u128) -> RemoteFileId {
        RemoteFileId(Uuid::from_u128(file))
    }

    fn node(n: u128) -> NodeId {
        NodeId(Uuid::from_u128(n))
    }

    /// What is known was learnt longer ago than `limit`, by both clocks; false where this
    /// computer's clocks have no such time (nothing to check then).
    fn aged(folders: &Folders, limit: Duration) -> bool {
        let ago = limit + Duration::from_secs(1);
        let (Some(steady), Some(wall)) = (
            Instant::now().checked_sub(ago),
            SystemTime::now().checked_sub(ago),
        ) else {
            return false;
        };
        for known in folders.state().folders.values_mut() {
            known.learnt = At(steady, wall);
        }
        true
    }

    /// The read on its way answers that `folder` is the file `at`, shared as `shares`.
    fn answer(folders: &Folders, folder: NodeId, at: RemoteRef, shares: Shares) {
        folders.read(folder, Some((Some(at), shares)));
    }

    #[test]
    fn a_folder_stays_known_until_the_feed_says_it_changed() {
        let folders = Folders::default();
        let (a, b) = (node(1), node(2));
        let shares: Shares = Arc::from([share(7, None)]);
        assert_eq!(folders.known(a), None);
        folders.reading();
        answer(&folders, a, at(100, 10), Arc::clone(&shares));
        folders.made(b, at(200, 20), Shares::default());
        assert_eq!(folders.known(a), Some(Arc::clone(&shares)));
        assert_eq!(folders.known(b).map(|s| s.len()), Some(0));

        // Its own write coming back, and another folder's row, change nothing.
        folders.fed(fed(a, 10, true));
        folders.fed(fed(node(3), 30, false));
        assert!(folders.known(a).is_some() && folders.known(b).is_some());

        // A share given or withdrawn moves `updated` alone; a write gives another version.
        folders.fed(fed(a, 10, false));
        assert_eq!(folders.known(a), None);
        folders.fed(fed(b, 21, true));
        assert_eq!(folders.known(b), None);
    }

    /// This computer's own write of a folder is no news in the feed, where the
    /// notifications heard every change of the folder so far; else the folder is read
    /// again: a share's change they missed moved `updated` as this write does.
    #[test]
    fn a_folder_written_here_stays_known_only_where_all_was_heard() {
        let folders = Folders::default();
        let (a, b) = (node(1), node(2));
        folders.made(a, at(100, 10), Shares::default());
        folders.rewritten(a, at(100, 11));
        assert_eq!(folders.known(a), None, "learnt while they were down");
        folders.listening(true);
        folders.made(a, at(100, 10), Shares::default());
        folders.rewritten(a, at(100, 11));
        folders.fed(fed(a, 11, true));
        assert!(folders.known(a).is_some());
        folders.rewritten(b, at(200, 21));
        assert_eq!(folders.known(b), None, "not known: nothing to keep");
        folders.listening(false);
        folders.rewritten(a, at(100, 12));
        assert_eq!(folders.known(a), None, "the connection broke since");
    }

    #[test]
    fn the_notifications_word_of_a_known_folder_has_it_read_again() {
        let folders = Folders::default();
        let (a, b) = (node(1), node(2));
        folders.made(a, at(100, 10), Arc::from([share(7, None)]));
        folders.made(b, at(200, 20), Shares::default());
        folders.heard(file(300));
        assert!(folders.known(a).is_some() && folders.known(b).is_some());
        folders.heard(file(100));
        assert!(folders.known(a).is_none() && folders.known(b).is_some());
        // Made again as another file: the file it was says nothing of it any more.
        folders.made(b, at(201, 22), Shares::default());
        folders.heard(file(200));
        assert!(folders.known(b).is_some());
        folders.heard(file(201));
        assert!(folders.known(b).is_none());
        assert!(folders.state().by_file.is_empty());
    }

    /// A read isn't kept where something was told of its folder while it was on its way:
    /// by the notifications (they name its file, which only the read's answer ties to the
    /// folder), by the feed, or by the notifications connecting; nor a second answer.
    /// What is told is kept for the read on its way alone, and no more than [`TOLD`].
    #[test]
    fn a_read_that_a_change_overtook_is_not_kept() {
        let folders = Folders::default();
        let a = node(1);
        let shares = || -> Shares { Arc::from([share(7, None)]) };
        let read = |told: &dyn Fn(&Folders)| {
            folders.reading();
            told(&folders);
            answer(&folders, a, at(100, 10), shares());
            let kept = folders.known(a).is_some();
            folders.state().forget(a);
            kept
        };
        assert!(read(&|_| {}));
        assert!(read(&|f| f.heard(file(300))), "another file's notice");
        assert!(read(&|f| f.fed(fed(node(2), 20, false))), "another folder");
        assert!(!read(&|f| f.heard(file(100))), "its file's notice");
        assert!(!read(&|f| f.fed(fed(a, 10, true))), "a feed row of it");
        assert!(!read(&|f| f.listening(true)), "connected meanwhile");
        assert!(!read(&|f| f.listening(false)), "the connection broke");
        let many = |f: &Folders| {
            (0..=TOLD as u128).for_each(|n| f.heard(file(1000 + n)));
            let state = f.state();
            let told = state.reading.as_ref().map(|reading| reading.heard.len());
            assert_eq!(told, Some(TOLD), "no more are kept");
        };
        assert!(!read(&many), "more told than is kept");
        // Without a read begun, an answer is nobody's; and nothing is kept of what is told.
        answer(&folders, a, at(100, 10), shares());
        assert!(folders.known(a).is_none());
        folders.heard(file(100));
        assert!(folders.state().reading.is_none());
        // A read that ended without an answer isn't on its way any more.
        folders.reading();
        folders.read(a, None);
        assert!(folders.state().reading.is_none() && folders.known(a).is_none());
    }

    /// Learnt while the notifications are connected, a folder stays known for as long as
    /// that connection holds and this computer doesn't sleep; learnt before, or once it
    /// broke, for [`KEPT`] by both clocks.
    #[test]
    fn what_was_learnt_is_kept_while_a_change_would_be_told() {
        let folders = Folders::default();
        let (a, b) = (node(1), node(2));
        folders.made(a, at(100, 10), Shares::default());
        folders.listening(true);
        folders.made(b, at(200, 20), Shares::default());
        if !aged(&folders, KEPT) {
            return;
        }
        assert!(folders.known(a).is_none(), "learnt before they connected");
        assert!(folders.known(b).is_some(), "connected all the while");
        folders.listening(false);
        assert!(folders.known(b).is_none(), "the connection broke");
        folders.listening(true);
        assert!(folders.known(b).is_none(), "another: something was missed");

        // A sleep since (the wall clock ahead of the steady one): the connection heard
        // nothing meanwhile, whatever it says now.
        let slept = |by: Duration| {
            folders.made(a, at(100, 10), Shares::default());
            let wall = SystemTime::now().checked_sub(by).expect("past 1970");
            for known in folders.state().folders.values_mut() {
                known.learnt.1 = wall;
            }
            folders.known(a).is_some()
        };
        assert!(slept(SLEPT / 2), "the clocks' own drift");
        assert!(
            slept(SLEPT * 2),
            "a nap: for the ten minutes, as when they are down"
        );
        assert!(!slept(KEPT + SLEPT * 2), "a night");
    }

    /// A folder whose row isn't there, or doesn't read, is kept as sharing nothing for
    /// [`KEPT`], connected or not: the notifications can't name it, the feed can.
    #[test]
    fn a_folder_that_does_not_read_shares_nothing_for_a_while() {
        let folders = Folders::default();
        let a = node(1);
        folders.listening(true);
        folders.reading();
        folders.read(a, Some((None, Shares::default())));
        assert_eq!(folders.known(a).map(|s| s.len()), Some(0));
        assert!(folders.state().by_file.is_empty());
        folders.fed(fed(a, 10, true));
        assert!(
            folders.known(a).is_none(),
            "a row of it in the feed: asked again"
        );
        folders.reading();
        folders.read(a, Some((None, Shares::default())));
        if aged(&folders, KEPT) {
            assert!(folders.known(a).is_none(), "asked again");
        }
    }

    /// What is added to a folder whose shares the server refused goes up without them for
    /// an hour, or until the folder changes; only where the shares refused are still what
    /// is known of it. What it shares stays known as it is (a move out of it withdraws
    /// that), and the hour is the refusal's: the folder read again meanwhile, shared as it
    /// was, is still refused. Refused without them too, it wasn't for the shares.
    #[test]
    fn a_folder_whose_shares_were_refused_gives_none_for_a_while() {
        let folders = Folders::default();
        let a = node(1);
        let sent: Shares = Arc::from([share(7, None)]);
        assert!(!folders.refused(a, &sent), "not known: read again");
        folders.listening(true);
        folders.made(a, at(100, 10), Arc::clone(&sent));
        assert!(!folders.is_refused(a, &sent));
        assert!(folders.refused(a, &sent) && folders.is_refused(a, &sent));
        assert_eq!(folders.known(a), Some(Arc::clone(&sent)));
        // Read again (the notifications down, every ten minutes): refused still.
        folders.forget(a);
        folders.made(a, at(100, 10), Arc::from([share(7, None)]));
        assert!(folders.is_refused(a, &sent), "the same shares, read again");
        let long_ago = SystemTime::now().checked_sub(REFUSED + Duration::from_secs(1));
        for (at, _) in folders.state().refusals.values_mut() {
            *at = At(Instant::now(), long_ago.expect("an hour ago"));
        }
        assert!(!folders.is_refused(a, &sent), "tried again");

        folders.refused(a, &sent);
        folders.heard(file(100));
        assert!(
            folders.known(a).is_none() && !folders.is_refused(a, &sent),
            "it changed"
        );

        // Changed since the upload began: what it shares now wasn't refused.
        let now: Shares = Arc::from([share(8, None)]);
        folders.made(a, at(100, 10), Arc::clone(&now));
        assert!(!folders.refused(a, &sent) && !folders.is_refused(a, &now));

        folders.not_for_its_shares(a);
        assert!(folders.known(a).is_some(), "not refused: left as it is");
        assert!(folders.refused(a, &now));
        folders.not_for_its_shares(a);
        assert!(folders.known(a).is_none() && !folders.is_refused(a, &now));
    }

    #[test]
    fn folders_are_shared_alike_in_whatever_order() {
        let (a, b, ends) = (share(1, None), share(2, None), share(1, Some(5)));
        assert!(alike(&[], &[]) && alike(&[a, b], &[b, a]));
        assert!(!alike(&[a], &[]) && !alike(&[a], &[b]) && !alike(&[a, b], &[a]));
        assert!(!alike(&[a], &[ends]), "until another time");
        let edits = Share {
            permissions: crate::app::READ | crate::app::WRITE,
            ..a
        };
        assert!(!alike(&[a], &[edits]), "for more");
    }

    #[test]
    fn many_known_folders_drop_the_ones_not_gone_by() {
        let folders = Folders::default();
        let one_more = MANY as u128 + 1;
        for n in 0..MANY as u128 {
            folders.made(node(n + 1), at(n + 1, 1), Shares::default());
        }
        if !aged(&folders, KEPT) {
            return;
        }
        folders.made(node(one_more), at(one_more, 1), Shares::default());
        let mut state = folders.state();
        assert_eq!((state.folders.len(), state.by_file.len()), (1, 1));
        // Heard all the while, all are still gone by: the quarter asked about longest ago
        // goes, and one asked about since stays.
        state.folders.clear();
        state.by_file.clear();
        drop(state);
        folders.listening(true);
        for n in 0..MANY as u128 {
            folders.made(node(n + 1), at(n + 1, 1), Shares::default());
        }
        assert!(aged(&folders, KEPT));
        assert!(
            folders.known(node(1)).is_some(),
            "the first learnt, asked about last"
        );
        folders.made(node(one_more), at(one_more, 1), Shares::default());
        let state = folders.state();
        let kept = MANY - MANY / 4 + 1;
        assert_eq!((state.folders.len(), state.by_file.len()), (kept, kept));
        assert!(state.folders.contains_key(&node(1)) && !state.folders.contains_key(&node(2)));
    }

    #[test]
    fn a_share_lasts_past_the_upload_or_is_not_given() {
        let end = 5_000_000_000;
        let ends = share(1, Some(end));
        assert!(ends.lasts(end - ENDING - 1, 0));
        assert!(!ends.lasts(end - ENDING, 0), "about to end");
        // A file of a gigabyte is taken to need over half an hour.
        let big = 1u64 << 30;
        let needs = big as i64 / SENT_A_MS;
        assert!(needs > 30 * 60 * 1000);
        assert!(!ends.lasts(end - ENDING - needs, big));
        assert!(ends.lasts(end - ENDING - needs - 1, big));
        assert!(share(1, None).lasts(i64::MAX, u64::MAX));
    }
}
