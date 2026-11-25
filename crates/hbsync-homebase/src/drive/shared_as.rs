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
//! by the notifications while they are connected ([`Folders::heard`]), by the feed for what
//! they missed ([`Folders::fed`], [`Folders::feed_ended`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

use hbsync_types::{NodeId, RemoteFileId, RemoteRef};
use uuid::Uuid;

/// How long what was learnt of a folder's shares is gone by where the notifications weren't
/// connected all the while since ([`Folders::known`]): the feed, read every few minutes
/// then, tells of a change ([`Folders::fed`]); this bounds the time before it does, at one
/// small read per folder added to in that time.
const KEPT: Duration = Duration::from_secs(600);

/// How long a folder whose shares the server refused with an upload is added to without
/// them ([`Folders::refused`]), unless it changes before: then they are read and tried again.
const REFUSED: Duration = Duration::from_secs(3600);

/// A feed read begun this long after the notifications connected, and read to its end,
/// holds every change they missed before: twice what the feed holds back
/// (`HOLD_BACK`), so nothing falls between the two.
const SETTLED: Duration = Duration::from_millis(2500);

/// More folders than this known: the ones no longer gone by are dropped as the next is
/// learnt (about 150 bytes each).
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
}

/// What is known of one folder.
#[derive(Debug)]
struct Known {
    /// The folder's file, which the notifications name it by.
    file_id: Uuid,
    /// The folder's version as its shares were read, or as this computer made it.
    version: Uuid,
    shares: Shares,
    learnt: At,
    /// The notifications' connection that vouches for it: up as it was learnt, or as the
    /// feed was read to its end since ([`Folders::feed_ended`]).
    listening: Option<u64>,
    /// The server refused its shares with an upload ([`Folders::refused`]): none are given.
    refused: bool,
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

#[derive(Debug, Default)]
struct State {
    folders: HashMap<NodeId, Known>,
    /// The known folders by their files ([`Known::file_id`]).
    by_file: HashMap<Uuid, NodeId>,
    /// The notifications' connections so far, and the one that is up now, with when it
    /// came up ([`Folders::listening`]).
    connects: u64,
    listening: Option<(u64, Instant)>,
    reading: Option<Reading>,
    /// As many folders known has the ones not gone by any more dropped ([`MANY`], then
    /// twice what was left).
    full: usize,
}

impl State {
    fn connection(&self) -> Option<u64> {
        self.listening.map(|(connection, _)| connection)
    }

    /// Whether what is `known` of a folder is still gone by ([`Folders::known`]).
    fn holds(&self, known: &Known) -> bool {
        if known.refused {
            return known.learnt.within(REFUSED);
        }
        let vouched = known.listening.is_some() && known.listening == self.connection();
        vouched || known.learnt.within(KEPT)
    }

    /// Nothing is known of `folder` any more.
    fn forget(&mut self, folder: NodeId) {
        if let Some(known) = self.folders.remove(&folder) {
            self.by_file.remove(&known.file_id);
        }
    }

    fn learn(&mut self, folder: NodeId, at: RemoteRef, shares: Shares) {
        if self.folders.len() >= self.full.max(MANY) {
            let mut folders = std::mem::take(&mut self.folders);
            folders.retain(|_, known| self.holds(known));
            self.by_file
                .retain(|_, folder| folders.contains_key(folder));
            // (Where most are still gone by, not at every folder learnt from here on.)
            self.full = folders.len() * 2;
            self.folders = folders;
        }
        let known = Known {
            file_id: at.file_id.0,
            version: at.version.0,
            shares,
            learnt: At::now(),
            listening: self.connection(),
            refused: false,
        };
        if let Some(before) = self.folders.insert(folder, known) {
            self.by_file.remove(&before.file_id);
        }
        self.by_file.insert(at.file_id.0, folder);
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

/// Where a read of the feed began ([`Folders::feed_begins`]).
#[derive(Debug, Clone, Copy)]
pub(crate) struct FeedRead {
    listening: Option<(u64, Instant)>,
    began: Instant,
}

impl Folders {
    fn state(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `folder`'s shares, if a change of them since they were learnt would have been told:
    /// vouched for by the notifications' connection that is up (a change of the folder is
    /// announced within seconds, [`Folders::heard`]), else for [`KEPT`]. A folder whose
    /// shares the server refused has none for [`REFUSED`].
    pub(crate) fn known(&self, folder: NodeId) -> Option<Shares> {
        let state = self.state();
        let known = state.folders.get(&folder)?;
        (state.holds(known)).then(|| Arc::clone(&known.shares))
    }

    /// The notifications connected (`true`), or their connection broke. What they missed
    /// meanwhile the feed tells: what was learnt before is vouched for again once it has
    /// been read to its end ([`Folders::feed_ended`]).
    pub(crate) fn listening(&self, connected: bool) {
        let mut state = self.state();
        state.listening = connected.then(|| {
            state.connects += 1;
            (state.connects, Instant::now())
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
            listening: state.connection(),
            ..Reading::default()
        });
    }

    /// The read that began ([`Folders::reading`]) says `folder` (the file `at`) is shared as
    /// `shares`: kept, unless something was told of the folder meanwhile, or the
    /// notifications' connection isn't the one it began under.
    pub(crate) fn read(&self, folder: NodeId, at: RemoteRef, shares: Shares) {
        let mut state = self.state();
        let Some(reading) = state.reading.take() else {
            return;
        };
        let overtaken = reading.overtaken
            || reading.listening != state.connection()
            || reading.heard.contains(&at.file_id.0)
            || reading.fed.contains(&folder);
        if !overtaken {
            state.learn(folder, at, shares);
        }
    }

    /// This computer made `folder` (the file `at`), shared as `shares`.
    pub(crate) fn made(&self, folder: NodeId, at: RemoteRef, shares: Shares) {
        self.state().learn(folder, at, shares);
    }

    /// This computer wrote `folder`, known or not, to the version `at`: its shares are as
    /// they were (a write keeps them), and its own write coming back in the feed is no news.
    pub(crate) fn rewritten(&self, folder: NodeId, at: RemoteRef) {
        let mut state = self.state();
        if let Some(known) = state.folders.get_mut(&folder)
            && known.file_id == at.file_id.0
        {
            known.version = at.version.0;
        }
    }

    /// The server refused an upload into `folder` that carried the folder's shares: what is
    /// added to it goes up without them for [`REFUSED`], or until the folder changes.
    /// Whether the folder was known (else its shares are read again, and tried again).
    pub(crate) fn refused(&self, folder: NodeId) -> bool {
        let mut state = self.state();
        let Some(known) = state.folders.get_mut(&folder) else {
            return false;
        };
        known.shares = Shares::default();
        known.learnt = At::now();
        known.refused = true;
        true
    }

    /// The notifications named `file`, or the feed has a row of it that doesn't read: if it
    /// is a known folder, it is read again when next added to. The notifications announce
    /// what others did (this computer's own writes aren't announced to it), and a share
    /// given or withdrawn is announced as a change that keeps the folder's version
    /// (**[verified]** live 2026-11-25): any word of a known folder counts.
    pub(crate) fn heard(&self, file: RemoteFileId) {
        let mut state = self.state();
        if let Some(folder) = state.by_file.get(&file.0).copied() {
            state.forget(folder);
        }
        if let Some(reading) = &mut state.reading {
            reading.overtaken |= reading.heard.len() >= TOLD;
            reading.heard.push(file.0);
        }
    }

    /// A feed row of a folder: one that isn't as it was learnt (another version, or changed
    /// since its version was written, as a share given or withdrawn changes it) is read
    /// again when next added to. This computer's own write of a folder comes back as it was
    /// made, and stays known.
    pub(crate) fn fed(&self, row: Fed) {
        let mut state = self.state();
        let same = |known: &Known| row.as_written && known.version == row.version;
        if state
            .folders
            .get(&row.node)
            .is_some_and(|known| !same(known))
        {
            state.forget(row.node);
        }
        if let Some(reading) = &mut state.reading {
            reading.overtaken |= reading.fed.len() >= TOLD;
            reading.fed.push(row.node);
        }
    }

    /// A read of the feed begins: what [`Folders::feed_ended`] is given back.
    pub(crate) fn feed_begins(&self) -> FeedRead {
        FeedRead {
            listening: self.state().listening,
            began: Instant::now(),
        }
    }

    /// The feed was read to its end, each row of a folder told ([`Folders::fed`]). Begun
    /// [`SETTLED`] after the notifications' connection that is still up came up, it held
    /// every change made before they listened: what is known now is as the drive has it,
    /// and that connection vouches for it from here on.
    pub(crate) fn feed_ended(&self, read: FeedRead) {
        let mut state = self.state();
        let (Some((connection, since)), Some(now)) = (read.listening, state.connection()) else {
            return;
        };
        if connection != now || read.began.duration_since(since) < SETTLED {
            return;
        }
        for known in state.folders.values_mut() {
            known.listening = Some(connection);
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

    /// The notifications connected long enough ago for a feed read begun now to vouch.
    fn settled(folders: &Folders) -> bool {
        let Some(since) = Instant::now().checked_sub(SETTLED) else {
            return false;
        };
        if let Some((_, connected)) = &mut folders.state().listening {
            *connected = since;
        }
        true
    }

    #[test]
    fn a_folder_stays_known_until_the_feed_says_it_changed() {
        let folders = Folders::default();
        let (a, b) = (node(1), node(2));
        let shares: Shares = Arc::from([share(7, None)]);
        assert_eq!(folders.known(a), None);
        folders.reading();
        folders.read(a, at(100, 10), Arc::clone(&shares));
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

        // This computer's own write of a known folder: its new version coming back is no news.
        folders.made(b, at(200, 20), Shares::default());
        folders.rewritten(b, at(200, 23));
        folders.fed(fed(b, 23, true));
        assert!(folders.known(b).is_some());
        folders.rewritten(a, at(100, 11));
        assert_eq!(folders.known(a), None, "not known: nothing to keep");
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
    }

    /// A read isn't kept where something was told of its folder while it was on its way:
    /// by the notifications (they name its file, which only the read's answer ties to the
    /// folder), by the feed, or by the notifications connecting; nor a second answer.
    #[test]
    fn a_read_that_a_change_overtook_is_not_kept() {
        let folders = Folders::default();
        let a = node(1);
        let shares = || -> Shares { Arc::from([share(7, None)]) };
        let read = |told: &dyn Fn(&Folders)| {
            folders.reading();
            told(&folders);
            folders.read(a, at(100, 10), shares());
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
        let many = |f: &Folders| (0..=TOLD as u128).for_each(|n| f.heard(file(1000 + n)));
        assert!(!read(&many), "more told than is kept");
        // Without a read begun, an answer is nobody's.
        folders.read(a, at(100, 10), shares());
        assert!(folders.known(a).is_none());
    }

    /// Learnt while the notifications are connected, a folder stays known for as long as
    /// they are; learnt before, or once they broke, for [`KEPT`] by both clocks. A feed read
    /// to its end, begun once the new connection settled, vouches for what is known again.
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

        // A feed read begun as they connect may lack what the feed still holds back.
        folders.feed_ended(folders.feed_begins());
        assert!(folders.known(b).is_none());
        // One begun before another connection says nothing of this one.
        assert!(settled(&folders));
        let before = folders.feed_begins();
        folders.listening(true);
        assert!(settled(&folders));
        folders.feed_ended(before);
        assert!(folders.known(b).is_none());
        // Begun once it settled, and read to its end: all that is known is as the drive is.
        folders.feed_ended(folders.feed_begins());
        assert!(folders.known(a).is_some() && folders.known(b).is_some());

        // The wall clock alone past it (a sleep, which the steady clock doesn't count).
        folders.listening(false);
        folders.made(a, at(100, 10), Shares::default());
        assert!(folders.known(a).is_some());
        let long_ago = SystemTime::now().checked_sub(KEPT + Duration::from_secs(1));
        for known in folders.state().folders.values_mut() {
            known.learnt.1 = long_ago.expect("a wall clock past 1970");
        }
        assert!(folders.known(a).is_none());
    }

    /// A folder whose shares the server refused gives none, connected or not, until it
    /// changes or an hour passed.
    #[test]
    fn a_folder_whose_shares_were_refused_gives_none_for_a_while() {
        let folders = Folders::default();
        let a = node(1);
        assert!(!folders.refused(a), "not known: read again");
        folders.listening(true);
        folders.made(a, at(100, 10), Arc::from([share(7, None)]));
        assert!(folders.refused(a));
        assert_eq!(folders.known(a).map(|s| s.len()), Some(0));
        if aged(&folders, REFUSED) {
            assert!(folders.known(a).is_none(), "tried again");
        }
        folders.made(a, at(100, 10), Arc::from([share(7, None)]));
        folders.refused(a);
        folders.heard(file(100));
        assert!(
            folders.known(a).is_none(),
            "it changed: read and tried again"
        );
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
        assert_eq!(state.full, 0, "few left: as many as before fill it again");
        // Vouched for, none is dropped however long ago it was learnt.
        state.folders.clear();
        state.by_file.clear();
        drop(state);
        folders.listening(true);
        for n in 0..MANY as u128 {
            folders.made(node(n + 1), at(n + 1, 1), Shares::default());
        }
        assert!(aged(&folders, KEPT));
        folders.made(node(one_more), at(one_more, 1), Shares::default());
        let state = folders.state();
        assert_eq!(state.folders.len(), MANY + 1);
        assert_eq!(state.full, MANY * 2);
    }

    #[test]
    fn a_share_lasts_past_the_upload_or_is_not_given() {
        let ends = share(1, Some(5_000_000_000));
        let end = 5_000_000_000;
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
