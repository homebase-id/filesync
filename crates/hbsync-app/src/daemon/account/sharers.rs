//! Whose shares an account syncs, as its identity's share markers say, each opened on its
//! sharer's server with a token of its own that is kept (docs/spec/daemon.md, "Whose shares
//! are synced"): the daemon's [`Sharers`].

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use hbsync_engine::Drive;
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_homebase::drive::markers::Sharer;
use hbsync_homebase::drive::shared::PeerRemote;
use hbsync_remote::{
    Answers, ChangesPage, Deletes, ForGood, Holds, NodeMeta, RefsPage, Refusal, Remote,
    RemoteError, RemoteHeader, Upload,
};
use hbsync_types::{Doomed, NodeId, RemoteFileId, Target, Written};
use uuid::Uuid;

use super::feed::{Notifications, Puller};
use super::parts::{Share, Sharers, Synced};
use crate::runner::RunnerError;
use crate::{Identity, Paths};

/// The file a sharer's token is kept in, beside the state DB of what they share: it goes
/// with those records when the share ends.
const TOKEN: &str = "token";

/// A sharer as a marker names them, if that is an identity's address and nothing else: host
/// labels of letters, digits and `-` joined by dots, and a port at most. What a marker's
/// content says becomes a folder's name (the share's records, `Paths::shared_db`), so
/// nothing with a separator, a `..` or an empty label is taken for one. With no `_` in it,
/// [`Identity::file_key`] is read back exactly ([`of_file_key`]).
pub(crate) fn sharer_name(odin_id: &str) -> Option<Identity> {
    let (host, port) = match odin_id.split_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (odin_id, None),
    };
    let label = |l: &str| {
        (1..=63).contains(&l.len())
            && l.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    };
    let port_ok =
        port.is_none_or(|p| (1..=5).contains(&p.len()) && p.bytes().all(|b| b.is_ascii_digit()));
    (host.len() <= 253 && host.contains('.') && host.split('.').all(label) && port_ok)
        .then(|| Identity::new(odin_id))
}

/// The sharer whose records are kept in a folder named `key` ([`Identity::file_key`]), if
/// it is one's.
fn of_file_key(key: &str) -> Option<Identity> {
    let sharer = sharer_name(&key.replace('_', ":"))?;
    (sharer.file_key() == key).then_some(sharer)
}

/// The account's shares by its identity's markers.
#[derive(Debug)]
pub(crate) struct ByMarkers {
    me: Identity,
    own: Arc<HomebaseRemote>,
    paths: Paths,
    /// The drive each sharer's files are on, as the markers last named it.
    drives: Mutex<HashMap<Identity, Uuid>>,
}

impl ByMarkers {
    pub(crate) fn new(me: &Identity, own: Arc<HomebaseRemote>, paths: &Paths) -> ByMarkers {
        ByMarkers {
            me: me.clone(),
            own,
            paths: paths.clone(),
            drives: Mutex::default(),
        }
    }

    /// Whose shares have records on this device: a state DB in a folder named as a sharer.
    fn kept(&self) -> std::io::Result<Vec<Identity>> {
        // (Any sharer's records: their folder is the one to list.)
        let db = self.paths.shared_db(&self.me, &self.me);
        let Some(dir) = db.parent().and_then(Path::parent) else {
            return Ok(Vec::new());
        };
        kept_in(dir)
    }
}

/// The sharers with a state DB in `dir`, the folder of an account's shares' records.
fn kept_in(dir: &Path) -> std::io::Result<Vec<Identity>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut kept = Vec::new();
    for entry in entries {
        let entry = entry?;
        let sharer = entry.file_name().to_str().and_then(of_file_key);
        if let Some(sharer) = sharer
            && entry.path().join("state.db").exists()
        {
            kept.push(sharer);
        }
    }
    kept.sort();
    Ok(kept)
}

impl Sharers for ByMarkers {
    fn told(&self, synced: &[Identity]) -> Result<Synced, RunnerError> {
        let markers = self
            .own
            .markers()
            .map_err(hbsync_engine::EngineError::Remote)?;
        let mut on = Vec::new();
        let mut drives = HashMap::new();
        for sharer in &markers.synced {
            match sharer_name(&sharer.odin_id) {
                // (Its own marker for itself syncs nothing: its drive is the account's.)
                Some(name) if name != self.me => {
                    if drives.insert(name.clone(), sharer.drive_alias).is_none() {
                        on.push(name);
                    }
                }
                Some(_) => {}
                None => tracing::warn!(
                    "a share marker names {:?}, which isn't an identity: not synced",
                    sharer.odin_id
                ),
            }
        }
        *self.drives.lock().unwrap_or_else(|e| e.into_inner()) = drives;
        let kept = self.kept()?;
        // Of those synced, being opened or kept: whose marker may be one that didn't read.
        let mut seen = HashSet::new();
        let unsure = synced
            .iter()
            .chain(&kept)
            .filter(|s| seen.insert(*s) && markers.unsure(s))
            .cloned()
            .collect();
        Ok(Synced { on, unsure, kept })
    }

    fn open(&self, sharer: &Identity, to_end: bool) -> Result<Share, RunnerError> {
        let db = self.paths.shared_db(&self.me, sharer);
        let dir = db.parent().unwrap_or(&db);
        let (remote, login): (Arc<dyn Remote>, Option<String>) = if to_end {
            // Nothing is asked of them: a share that ended gives no access any more.
            (Arc::new(Ended(sharer.clone())), None)
        } else {
            let drives = self.drives.lock().unwrap_or_else(|e| e.into_inner());
            let alias = drives.get(sharer).copied().ok_or_else(|| {
                std::io::Error::other(format!("no share marker names {sharer}'s drive"))
            })?;
            drop(drives);
            std::fs::create_dir_all(dir)?;
            let sharer = Sharer {
                odin_id: sharer.to_string(),
                drive_alias: alias,
            };
            let peer = PeerRemote::open(Arc::clone(&self.own), sharer, &dir.join(TOKEN))
                .map_err(hbsync_engine::EngineError::Remote)?;
            let login = peer.login_id().to_owned();
            (Arc::new(peer), Some(login))
        };
        let drive = Drive::open(&db, &self.me, remote)?;
        // The files' tokens are sealed under the token's secret: another token's don't open.
        if let Some(login) = login
            && drive.tokens_sealed_by(&login)?
        {
            tracing::info!("a new token for {sharer}'s server: the files' tokens are read again");
        }
        Ok(Share {
            sharer: sharer.clone(),
            notifications: Notifications::new(Puller::spawn(Arc::clone(&drive)), Instant::now()),
            drive,
            db,
        })
    }
}

/// A share that ended, opened only to take its copy off this device: its sharer's drive,
/// which answers nothing any more. Nothing is asked of it.
#[derive(Debug)]
struct Ended(Identity);

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

    /// Only an identity's address is a sharer's name: what a marker says becomes a folder's
    /// name.
    #[test]
    fn only_an_address_is_a_sharers_name() {
        for name in [
            "tom.example",
            "tom.dotyou.cloud",
            "a-b.example.com",
            "tom.example:4444",
            "1.example",
        ] {
            assert_eq!(sharer_name(name).as_deref(), Some(name), "{name}");
        }
        for name in [
            "",
            ".",
            "..",
            "tom",
            "tom..example",
            ".tom.example",
            "tom.example.",
            "../tom.example",
            "tom.example/..",
            "tom\\example.com",
            "Tom.Example",
            "tom_x.example",
            "tom.example:",
            "tom.example:44x4",
            "tom.example:444444",
            "tom.example:1:2",
            "tom .example",
            "tøm.example",
        ] {
            assert_eq!(sharer_name(name), None, "{name:?}");
        }
        let long = format!("{}.example", "a".repeat(64));
        assert_eq!(sharer_name(&long), None);
    }

    /// The records kept are those in a sharer's folder holding a state DB; a folder's name
    /// is read back as the sharer it was made for, a port's too.
    #[test]
    fn the_records_kept_are_read_back_by_their_folders_names() -> std::io::Result<()> {
        let dir = tempfile::tempdir()?;
        assert_eq!(kept_in(&dir.path().join("none"))?, []);
        let with_db = |name: &str| -> std::io::Result<()> {
            std::fs::create_dir(dir.path().join(name))?;
            std::fs::write(dir.path().join(name).join("state.db"), "")
        };
        with_db("tom.example")?;
        with_db("sam.example_4444")?;
        // Not a sharer's name, and a sharer's folder without a DB (opened, never synced).
        with_db("not a name")?;
        std::fs::create_dir(dir.path().join("pippin.example"))?;
        std::fs::write(dir.path().join("pippin.example").join(TOKEN), "")?;
        let kept: Vec<Identity> = vec!["sam.example:4444".into(), "tom.example".into()];
        assert_eq!(kept_in(dir.path())?, kept);
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
