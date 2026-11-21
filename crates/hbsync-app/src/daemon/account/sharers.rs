//! Whose shares an account syncs, as its identity's share markers say, each opened on its
//! sharer's server with a token of its own that is kept (docs/spec/daemon.md, "Whose shares
//! are synced"): the daemon's [`Sharers`].

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::ops::Range;
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex};

use hbsync_engine::Drive;
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::drive::markers::{Sharer, is_domain};
use hbsync_homebase::drive::shared::PeerRemote;
use hbsync_homebase::notify::Notifier;
use hbsync_remote::{
    Answers, ChangesPage, Deletes, ForGood, Holds, NodeMeta, RefsPage, Refusal, Remote,
    RemoteError, RemoteHeader, Upload,
};
use hbsync_types::{Doomed, NodeId, RemoteFileId, Target, Written};
use uuid::Uuid;

use super::Msg;
use super::parts::{Share, Sharers, Synced};
use crate::runner::RunnerError;
use crate::{Identity, Paths};

/// The sharer whose records are kept in a folder named `key`, if it is one's: a marker
/// names a domain and nothing else ([`is_domain`]), which is its own file key.
fn of_file_key(key: &str) -> Option<Identity> {
    is_domain(key).then(|| Identity::new(key))
}

/// The account's shares by its identity's markers.
#[derive(Debug)]
pub(crate) struct ByMarkers {
    me: Identity,
    own: Arc<HomebaseRemote>,
    paths: Paths,
    /// The drive each sharer's files are on, as the markers last named it.
    drives: Mutex<HashMap<Identity, Uuid>>,
    /// Where each share's notifications go: the account ([`Msg::ShareHint`]).
    to_account: Sender<Msg>,
}

impl ByMarkers {
    /// `to_account`: the account's own channel, which each share's notifications are sent
    /// on.
    pub(crate) fn new(
        me: &Identity,
        own: Arc<HomebaseRemote>,
        paths: &Paths,
        to_account: Sender<Msg>,
    ) -> ByMarkers {
        ByMarkers {
            me: me.clone(),
            own,
            paths: paths.clone(),
            drives: Mutex::default(),
            to_account,
        }
    }

    /// `sharer`'s notifications, read on `peer` (their server), their events sent to the
    /// account under the sharer's name by a thread of their own, which ends with them.
    /// `None` where a thread couldn't start (told): the share is read on a timer.
    fn listen(&self, sharer: &Identity, peer: &Arc<PeerRemote>) -> Option<Notifier> {
        let (tx, events) = channel();
        let (to_account, from) = (self.to_account.clone(), sharer.clone());
        let forward = move || {
            for event in events {
                if to_account
                    .send(Msg::ShareHint(from.clone(), event))
                    .is_err()
                {
                    return;
                }
            }
        };
        let started = std::thread::Builder::new()
            .name("hbsync-hints".into())
            .spawn(forward)
            .and_then(|_| peer.notifier(tx));
        started
            .inspect_err(|e| {
                tracing::error!(
                    "can't start {sharer}'s notifications ({e}); reading what they share now \
                     and then"
                )
            })
            .ok()
    }
}

/// Whose shares `me`'s account has records of on this device: a state DB in a folder named
/// as a sharer.
fn kept(paths: &Paths, me: &Identity) -> std::io::Result<Vec<Identity>> {
    let entries = match std::fs::read_dir(paths.shares_dir(me)) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut kept = Vec::new();
    for entry in entries {
        let sharer = entry?.file_name().to_str().and_then(of_file_key);
        if let Some(sharer) = sharer
            && paths.shared_db(me, &sharer).exists()
        {
            kept.push(sharer);
        }
    }
    kept.sort();
    Ok(kept)
}

/// What the markers say of whose shares to sync (`on`: the sharers they turn on, each with
/// their drive; `unsure`: whether a marker that may be a sharer's didn't read), for an
/// account syncing or opening `synced` with records `kept`: [`Sharers::told`]'s answer, and
/// the drive of each one on. One who is unsure stays on the drive named `before`, if one
/// was: an opening of theirs under way goes on as it was told.
fn following(
    on: &[Sharer],
    unsure: impl Fn(&Identity) -> bool,
    synced: &[Identity],
    kept: Vec<Identity>,
    before: &HashMap<Identity, Uuid>,
) -> (Synced, HashMap<Identity, Uuid>) {
    let mut drives = HashMap::new();
    let mut named = Vec::new();
    for sharer in on {
        let name = Identity::new(&sharer.odin_id);
        // (One marker a sharer: a second, which the server doesn't keep, changes nothing.)
        if let Entry::Vacant(drive) = drives.entry(name.clone()) {
            drive.insert(sharer.drive_alias);
            named.push(name);
        }
    }
    // Of those synced, being opened or kept: whose marker may be one that didn't read.
    let unsure: Vec<Identity> = synced
        .iter()
        .chain(&kept)
        .filter(|s| unsure(s))
        .cloned()
        .collect();
    for sharer in &unsure {
        if let (Entry::Vacant(drive), Some(named)) =
            (drives.entry(sharer.clone()), before.get(sharer))
        {
            drive.insert(*named);
        }
    }
    let told = Synced {
        on: named,
        unsure,
        kept,
    };
    (told, drives)
}

impl Sharers for ByMarkers {
    fn told(&self, synced: &[Identity]) -> Result<Synced, RunnerError> {
        let markers = self
            .own
            .markers()
            .map_err(hbsync_engine::EngineError::Remote)?;
        let kept = kept(&self.paths, &self.me)?;
        let mut named = self.drives.lock().unwrap_or_else(|e| e.into_inner());
        let (told, drives) = following(
            &markers.synced,
            |sharer| markers.unsure(sharer),
            synced,
            kept,
            &named,
        );
        *named = drives;
        Ok(told)
    }

    fn open(&self, sharer: &Identity, to_end: bool) -> Result<Share, RunnerError> {
        let db = self.paths.shared_db(&self.me, sharer);
        if to_end {
            // Nothing is asked of them: a share that ended gives no access any more.
            let drive = Drive::open(&db, &self.me, Arc::new(Ended(sharer.clone())))?;
            return Ok(Share::new(sharer, drive, db));
        }
        let drives = self.drives.lock().unwrap_or_else(|e| e.into_inner());
        let drive_alias = drives.get(sharer).copied().ok_or_else(|| {
            std::io::Error::other(format!("no share marker names {sharer}'s drive"))
        })?;
        drop(drives);
        std::fs::create_dir_all(self.paths.shared_dir(&self.me, sharer))?;
        let named = Sharer {
            odin_id: sharer.to_string(),
            drive_alias,
        };
        let token = self.paths.shared_token(&self.me, sharer);
        let peer = PeerRemote::open(Arc::clone(&self.own), named, &token)
            .map_err(hbsync_engine::EngineError::Remote)?;
        let peer = Arc::new(peer);
        let login = peer.login_id();
        let drive = Drive::open(&db, &self.me, peer.clone())?;
        // The files' tokens are sealed under the token's secret: another token's don't open.
        if drive.tokens_sealed_by(&login)? {
            tracing::info!("a new token for {sharer}'s server: the files' tokens are read again");
        }
        let mut share = Share::new(sharer, drive, db);
        share.notifier = self.listen(sharer, &peer);
        Ok(share)
    }
}

/// A share that ended, opened only to take its copy off this device: its sharer's drive,
/// which answers nothing any more. Nothing is asked of it.
#[derive(Debug)]
pub(super) struct Ended(pub(super) Identity);

impl Ended {
    fn gone<T>(&self) -> hbsync_remote::Result<T> {
        let why = format!("{} no longer shares with you", self.0);
        Err(RemoteError::Denied(why, Refusal::Token))
    }
}

impl Remote for Ended {
    fn changes(&self, _: Option<&str>, _: usize) -> hbsync_remote::Result<ChangesPage> {
        self.gone()
    }

    fn change_refs(&self, _: Option<&str>, _: usize) -> hbsync_remote::Result<RefsPage> {
        self.gone()
    }

    fn cursor_time(&self, _: &str) -> Option<i64> {
        None
    }

    fn header(&self, _: RemoteFileId) -> hbsync_remote::Result<RemoteHeader> {
        self.gone()
    }

    fn holding(&self, _: &[NodeId], _: Holds<'_>) -> hbsync_remote::Result<HashSet<NodeId>> {
        self.gone()
    }

    fn shared(&self) -> bool {
        true
    }

    fn create(&self, _: &NodeMeta, _: Option<Upload<'_>>) -> hbsync_remote::Result<Written> {
        self.gone()
    }

    fn update(
        &self,
        _: &Target,
        _: &NodeMeta,
        _: Option<Upload<'_>>,
    ) -> hbsync_remote::Result<Written> {
        self.gone()
    }

    fn delete(&self, _: &[Doomed]) -> hbsync_remote::Result<Deletes> {
        self.gone()
    }

    fn delete_for_good(&self, _: &[Doomed]) -> hbsync_remote::Result<ForGood> {
        self.gone()
    }

    fn set_removed(&self, _: &[Target], _: bool) -> hbsync_remote::Result<Answers<Written>> {
        self.gone()
    }

    fn read(
        &self,
        _: RemoteFileId,
        _: Option<Range<u64>>,
        _: &mut dyn Write,
    ) -> hbsync_remote::Result<u64> {
        self.gone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sharer(odin_id: &str, drive: u128) -> Sharer {
        Sharer {
            odin_id: odin_id.into(),
            drive_alias: Uuid::from_u128(drive),
        }
    }

    /// Whom the markers turn on is synced, on the drive their marker names; of those synced
    /// or kept, the ones whose marker may not have read are unsure, and nobody else is.
    #[test]
    fn the_markers_say_who_is_on_and_who_is_unsure() {
        let on = [sharer("tom.example", 1), sharer("sam.example", 2)];
        let ids = |names: &[&str]| -> Vec<Identity> { names.iter().map(|n| (*n).into()).collect() };
        let synced = ids(&["tom.example", "ann.example"]);
        let kept = ids(&["ann.example", "bob.example", "tom.example"]);
        let unsure = |s: &Identity| ["ann.example", "sam.example"].contains(&s.as_str());
        let before = HashMap::from([
            (Identity::new("ann.example"), Uuid::from_u128(7)),
            (Identity::new("bob.example"), Uuid::from_u128(8)),
            (Identity::new("tom.example"), Uuid::from_u128(9)),
        ]);
        let (told, drives) = following(&on, unsure, &synced, kept.clone(), &before);
        assert_eq!(told.on, ids(&["tom.example", "sam.example"]));
        // (sam isn't synced, being opened or kept: of them nothing is to be kept as it is.
        // ann is both synced and kept: named for each, which nothing minds.)
        assert_eq!(told.unsure, ids(&["ann.example", "ann.example"]));
        assert_eq!(told.kept, kept);
        assert_eq!(drives[&Identity::new("tom.example")], Uuid::from_u128(1));
        assert_eq!(drives[&Identity::new("sam.example")], Uuid::from_u128(2));
        // ann, whose marker didn't read, stays on the drive named before; bob, who is off,
        // on none.
        assert_eq!(drives[&Identity::new("ann.example")], Uuid::from_u128(7));
        assert_eq!(drives.len(), 3);
    }

    /// A sharer named twice is one share, on the drive named first.
    #[test]
    fn a_sharer_named_twice_is_synced_once() {
        let on = [sharer("tom.example", 1), sharer("tom.example", 2)];
        let (told, drives) = following(&on, |_| false, &[], Vec::new(), &HashMap::new());
        assert_eq!(told.on, vec![Identity::new("tom.example")]);
        assert_eq!(drives[&Identity::new("tom.example")], Uuid::from_u128(1));
        assert_eq!(told.unsure, []);
    }

    /// The records kept are those in a sharer's folder holding a state DB: nothing else in
    /// the folder of an account's shares is one.
    #[test]
    fn the_records_kept_are_read_back_by_their_folders_names() -> std::io::Result<()> {
        let dir = tempfile::tempdir()?;
        let (paths, me) = (Paths::under(dir.path()), Identity::new("me.example"));
        assert_eq!(kept(&paths, &me)?, []);
        let shares = paths.shares_dir(&me);
        let with_db = |name: &str| -> std::io::Result<()> {
            std::fs::create_dir_all(shares.join(name))?;
            std::fs::write(shares.join(name).join("state.db"), "")
        };
        with_db("tom.example")?;
        with_db("sam.example")?;
        // Not a sharer's name (a marker names no port), and a sharer's folder without a DB
        // (opened, never synced).
        with_db("not a name")?;
        with_db("ann.example_4444")?;
        std::fs::create_dir(shares.join("pippin.example"))?;
        std::fs::write(paths.shared_token(&me, &"pippin.example".into()), "")?;
        let both: Vec<Identity> = vec!["sam.example".into(), "tom.example".into()];
        assert_eq!(kept(&paths, &me)?, both);
        Ok(())
    }

    /// A share that ended opens without its sharer: as a shared drive, asking nothing.
    #[test]
    fn a_share_that_ended_is_a_shared_drive_that_answers_nothing() {
        let ended = Ended("tom.example".into());
        assert!(ended.shared());
        assert!(matches!(
            ended.changes(None, 1),
            Err(RemoteError::Denied(_, Refusal::Token))
        ));
    }
}
