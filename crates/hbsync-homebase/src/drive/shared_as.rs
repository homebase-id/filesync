//! How this identity's own folders are shared, as far as this computer knows: what a file or
//! folder it adds to one is shared as (`docs/spec/homebase-api-notes.md`, "What the owner's
//! computer adds to a shared folder").
//!
//! A share is a row on each file, not on its folder: a new file reaches a folder's members
//! only with rows of its own. The app that shares writes them for what is there
//! (**[verified]** chat-kmp `FilesyncSharing.share`), and whoever adds a file writes the new
//! one's: this client with the upload itself, as the folder it goes into is shared.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use hbsync_types::{NodeId, RemoteFileId, RemoteRef};
use uuid::Uuid;

use crate::drive::share_keys::CircleKey;
use crate::drive::wire::ShareRow;

/// How long what was read of a folder's shares is gone by. The notifications tell of a change
/// within seconds ([`Folders::heard`]), the feed of what they missed ([`Folders::fed`]); this
/// bounds it where neither is read (the notifications down while requests still pass), at
/// one small read per folder added to in that time.
const KEPT: Duration = Duration::from_secs(600);

/// More folders than this known: the ones read longer ago than [`KEPT`] are dropped as the
/// next is learnt (they would be read again anyway).
const MANY: usize = 4096;

/// A share that ends within this isn't given to a new file: the server refuses a share whose
/// end has passed, and with it the upload carrying it (**[verified]** odin-core
/// `DriveShareService.AssertShape`), and this computer's clock isn't the server's.
const ENDING: i64 = 60_000;

/// One live share of a folder: the circle it is shared with, what the circle may do with it
/// ([`crate::app`]'s `READ`, `WRITE`, `REACT`, `COMMENT`), and when it ends (Unix ms).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Share {
    pub circle: Uuid,
    pub permissions: u32,
    pub expires_at: Option<i64>,
}

impl Share {
    /// A header's rows as shares: the live ones of a circle that reads. Its manager is sent
    /// the live rows alone (**[verified]** odin-core
    /// `DriveStorageServiceBase.AttachSharesAsync`); a row without the right to read is a
    /// withdrawn one.
    pub(crate) fn of(rows: &[ShareRow]) -> Shares {
        rows.iter()
            .filter(|row| row.permissions & crate::app::READ != 0)
            .filter_map(|row| {
                Some(Share {
                    circle: row.circle_id?,
                    permissions: row.permissions,
                    expires_at: row.expires_at,
                })
            })
            .collect()
    }

    /// Still to be given to a file added at `now` (Unix ms, this computer's clock).
    pub(crate) fn lasts(&self, now: i64) -> bool {
        self.expires_at
            .is_none_or(|end| end > now.saturating_add(ENDING))
    }
}

/// A folder's live shares; empty where it isn't shared.
pub(crate) type Shares = Arc<[Share]>;

/// What is known of one folder.
#[derive(Debug)]
struct Known {
    /// The folder's file, which the notifications name it by.
    file_id: Uuid,
    /// The folder's version as its shares were read, or as this computer made it.
    version: Uuid,
    shares: Shares,
    read: Instant,
}

#[derive(Debug, Default)]
struct State {
    /// Counts what was taken away: a read begun before isn't kept ([`Folders::learnt`]).
    changes: u64,
    folders: HashMap<NodeId, Known>,
    /// The known folders by their files ([`Known::file_id`]).
    by_file: HashMap<Uuid, NodeId>,
    /// The circles' keys, each read once: a circle's key is set once and stays
    /// (**[verified]** odin-core `CircleShareKeyAlreadySet`).
    circles: HashMap<Uuid, Arc<CircleKey>>,
}

impl State {
    /// Nothing is known of `folder` any more.
    fn forget(&mut self, folder: NodeId) {
        if let Some(known) = self.folders.remove(&folder) {
            self.by_file.remove(&known.file_id);
            self.changes += 1;
        }
    }
}

/// The folders of this identity's drive whose shares this computer has read, and the keys of
/// the circles they are shared with.
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

impl Folders {
    fn state(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `folder`'s shares, if read within [`KEPT`] and nothing was said of it since.
    pub(crate) fn known(&self, folder: NodeId) -> Option<Shares> {
        let state = self.state();
        let known = state.folders.get(&folder)?;
        (known.read.elapsed() < KEPT).then(|| Arc::clone(&known.shares))
    }

    /// Where a read of a folder's shares begins: what [`Folders::learnt`] is given back.
    pub(crate) fn reading(&self) -> u64 {
        self.state().changes
    }

    /// `folder` (the file `at`) is shared as `shares`: read in a request begun at `reading`,
    /// or (`None`) made so by this computer. A read that a change overtook isn't kept: what
    /// it read may be from before the change.
    pub(crate) fn learnt(
        &self,
        reading: Option<u64>,
        folder: NodeId,
        at: RemoteRef,
        shares: Shares,
    ) {
        let mut state = self.state();
        if reading.is_some_and(|reading| reading != state.changes) {
            return;
        }
        if state.folders.len() >= MANY {
            let State {
                folders, by_file, ..
            } = &mut *state;
            folders.retain(|_, known| known.read.elapsed() < KEPT);
            by_file.retain(|_, folder| folders.contains_key(folder));
        }
        let known = Known {
            file_id: at.file_id.0,
            version: at.version.0,
            shares,
            read: Instant::now(),
        };
        if let Some(before) = state.folders.insert(folder, known) {
            state.by_file.remove(&before.file_id);
        }
        state.by_file.insert(at.file_id.0, folder);
    }

    /// Nothing is known of `folder` any more.
    pub(crate) fn forget(&self, folder: NodeId) {
        self.state().forget(folder);
    }

    /// The notifications named `file`: if it is a known folder, it is read again when next
    /// added to. They announce what others did (this computer's own writes aren't announced
    /// to it), and a share given or withdrawn is announced as a change that keeps the
    /// folder's version (**[verified]** live 2026-11-25): any word of a known folder counts.
    pub(crate) fn heard(&self, file: RemoteFileId) {
        let mut state = self.state();
        if let Some(folder) = state.by_file.get(&file.0).copied() {
            state.forget(folder);
        }
    }

    /// The feed's rows: a folder that isn't as it was read (another version, or changed since
    /// its version was written, as a share given or withdrawn changes it) is read again when
    /// next added to. This computer's own write of a folder comes back as it was made, and
    /// stays known.
    pub(crate) fn fed(&self, rows: impl Iterator<Item = Fed>) {
        let mut state = self.state();
        if state.folders.is_empty() {
            return;
        }
        for row in rows {
            let same = |known: &Known| row.as_written && known.version == row.version;
            if state
                .folders
                .get(&row.node)
                .is_some_and(|known| !same(known))
            {
                state.forget(row.node);
            }
        }
    }

    /// `circle`'s key, if read before.
    pub(crate) fn circle(&self, circle: Uuid) -> Option<Arc<CircleKey>> {
        self.state().circles.get(&circle).cloned()
    }

    /// `circle`'s key, read.
    pub(crate) fn keep(&self, circle: Uuid, key: CircleKey) -> Arc<CircleKey> {
        let key = Arc::new(key);
        self.state().circles.insert(circle, Arc::clone(&key));
        key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn share(circle: u128, expires_at: Option<i64>) -> Share {
        Share {
            circle: Uuid::from_u128(circle),
            permissions: crate::app::READ,
            expires_at,
        }
    }

    /// The folder's file `file` at `version`.
    fn at(file: u128, version: u128) -> RemoteRef {
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

    #[test]
    fn a_folder_stays_known_until_the_feed_says_it_changed() {
        let folders = Folders::default();
        let (a, b) = (NodeId(Uuid::from_u128(1)), NodeId(Uuid::from_u128(2)));
        let shares: Shares = Arc::from([share(7, None)]);
        assert_eq!(folders.known(a), None);
        folders.learnt(Some(folders.reading()), a, at(100, 10), Arc::clone(&shares));
        folders.learnt(None, b, at(200, 20), Arc::from([]));
        assert_eq!(folders.known(a), Some(Arc::clone(&shares)));
        assert_eq!(folders.known(b).map(|s| s.len()), Some(0));

        // Its own write coming back, and another folder's row, change nothing.
        folders.fed([fed(a, 10, true), fed(NodeId(Uuid::from_u128(3)), 30, false)].into_iter());
        assert!(folders.known(a).is_some() && folders.known(b).is_some());

        // A share given or withdrawn moves `updated` alone; a write gives another version.
        folders.fed([fed(a, 10, false)].into_iter());
        assert_eq!(folders.known(a), None);
        folders.fed([fed(b, 21, true)].into_iter());
        assert_eq!(folders.known(b), None);
    }

    #[test]
    fn the_notifications_word_of_a_known_folder_has_it_read_again() {
        let folders = Folders::default();
        let (a, b) = (NodeId(Uuid::from_u128(1)), NodeId(Uuid::from_u128(2)));
        folders.learnt(None, a, at(100, 10), Arc::from([share(7, None)]));
        folders.learnt(None, b, at(200, 20), Arc::from([]));
        let reading = folders.reading();
        folders.heard(file(300));
        assert!(folders.known(a).is_some() && folders.known(b).is_some());
        assert_eq!(folders.reading(), reading, "another file: nothing changed");
        folders.heard(file(100));
        assert!(folders.known(a).is_none() && folders.known(b).is_some());
        assert_ne!(folders.reading(), reading);
        // Made again as another file: the file it was says nothing of it any more.
        folders.learnt(None, b, at(201, 22), Arc::from([]));
        folders.heard(file(200));
        assert!(folders.known(b).is_some());
        folders.heard(file(201));
        assert!(folders.known(b).is_none());
    }

    #[test]
    fn a_read_that_a_change_overtook_is_not_kept() {
        let folders = Folders::default();
        let (a, b) = (NodeId(Uuid::from_u128(1)), NodeId(Uuid::from_u128(2)));
        folders.learnt(None, b, at(200, 20), Arc::from([]));
        let reading = folders.reading();
        folders.fed([fed(b, 20, false)].into_iter());
        folders.learnt(Some(reading), a, at(100, 10), Arc::from([share(7, None)]));
        assert_eq!(folders.known(a), None, "read before the feed's word");
        folders.learnt(Some(folders.reading()), a, at(100, 10), Arc::from([]));
        assert!(folders.known(a).is_some());
        folders.forget(a);
        assert_eq!(folders.known(a), None);
        assert_ne!(folders.reading(), reading);
    }

    #[test]
    fn many_known_folders_drop_the_old_ones() {
        let folders = Folders::default();
        let node = |n: usize| NodeId(Uuid::from_u128(n as u128 + 1));
        for n in 0..MANY {
            folders.learnt(None, node(n), at(n as u128 + 1, 1), Arc::from([]));
        }
        // (A clock that started less than that ago has no such time: nothing to check.)
        let Some(long_ago) = Instant::now().checked_sub(KEPT + Duration::from_secs(1)) else {
            return;
        };
        for known in folders.state().folders.values_mut() {
            known.read = long_ago;
        }
        folders.learnt(None, node(MANY), at(MANY as u128 + 1, 1), Arc::from([]));
        let state = folders.state();
        assert_eq!((state.folders.len(), state.by_file.len()), (1, 1));
    }

    #[test]
    fn only_live_rows_that_read_are_shares() {
        let row = |circle: Option<u128>, permissions: u32, expires_at: Option<i64>| ShareRow {
            circle_id: circle.map(Uuid::from_u128),
            permissions,
            expires_at,
        };
        let shares = Share::of(&[
            row(
                Some(1),
                crate::app::READ | crate::app::WRITE,
                Some(5_000_000),
            ),
            row(Some(2), 0, None),
            row(None, crate::app::READ, None),
            row(Some(3), crate::app::WRITE, None),
        ]);
        assert_eq!(
            *shares,
            [Share {
                circle: Uuid::from_u128(1),
                permissions: crate::app::READ | crate::app::WRITE,
                expires_at: Some(5_000_000),
            }]
        );
        assert!(shares[0].lasts(5_000_000 - ENDING - 1));
        assert!(!shares[0].lasts(5_000_000 - ENDING), "about to end");
        assert!(share(1, None).lasts(i64::MAX));
    }
}
