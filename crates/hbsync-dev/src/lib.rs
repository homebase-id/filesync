//! Development tools around the sync engine and a live Homebase identity:
//! - `hbsync-register <identity>`: registers the app and authorizes this machine;
//! - `hbsync-sync <identity> <folder>`: syncs a folder with the identity's Filesync drive;
//! - `hbsync-clean <identity> --tombstones [--yes]`: counts the drive's tombstones, and with
//!   `--yes` deletes them for good (a test identity's, between runs).
//!
//! Not the product: that is the daemon `hbsyncd` (`hbsync-app`), with its config and the OS
//! keychain.

use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use hbsync_homebase::{Client, Credentials};
use hbsync_remote::{Remote, RemoteError};
use hbsync_types::{Doomed, RemoteRef};

/// Where the development tools keep the credentials of `identity`:
/// `$XDG_CONFIG_HOME/hbsync/dev/<identity>.json` (default `~/.config`). The product keeps them
/// in the OS keychain.
pub fn credentials_path(identity: &str) -> Result<PathBuf, String> {
    credentials_path_as(identity, None)
}

/// [`credentials_path`] of one of several clients of `identity` on this machine (`hbsync-register
/// --as <client>`): `<identity>.<client>.json`; `None` is the default one. Each is a client
/// registration of its own, as on another computer: a notification subscription leaves out
/// only its own token's writes (`excludeOwnWrites`), so test "computers" that should hear each
/// other need one each.
pub fn credentials_path_as(identity: &str, client: Option<&str>) -> Result<PathBuf, String> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .ok_or("no config folder (HOME unset)")?;
    let identity = hbsync_app::Identity::new(identity).file_key();
    let file = match client {
        None => format!("{identity}.json"),
        Some(c) => format!("{identity}.{c}.json"),
    };
    Ok(base.join("hbsync").join("dev").join(file))
}

/// The second client the live tests use for another computer.
pub const OTHER_CLIENT: &str = "b";

/// A client from the credentials `hbsync-register` saved for `identity`.
pub fn saved_client(identity: &str) -> Result<Client, Box<dyn std::error::Error>> {
    saved_client_as(identity, None)
}

/// The credentials file `hbsync-register` (`--as <client>`) saved for `identity`, or what to
/// run to make it.
pub fn credentials_or_hint(identity: &str, client: Option<&str>) -> Result<PathBuf, String> {
    let path = credentials_path_as(identity, client)?;
    if path.exists() {
        return Ok(path);
    }
    let register = match client {
        None => format!("hbsync-register {identity}"),
        Some(c) => format!("hbsync-register {identity} --as {c}"),
    };
    Err(format!(
        "no credentials for {identity} at {}: run {register} first",
        path.display()
    ))
}

/// A client from the credentials `hbsync-register --as <client>` saved for `identity`.
pub fn saved_client_as(
    identity: &str,
    client: Option<&str>,
) -> Result<Client, Box<dyn std::error::Error>> {
    let path = credentials_or_hint(identity, client)?;
    let creds = Credentials::load(&path)?.ok_or("the credentials file is gone")?;
    Ok(Client::new(&creds)?)
}

/// The value after `name` (say `--as`) in a tool's arguments.
pub fn option(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

/// `work` for each of `0..n` on 8 threads: how many went through, or the first error.
pub fn on_threads<E: std::fmt::Display>(
    n: usize,
    work: impl Fn(usize) -> Result<(), E> + Sync,
) -> Result<usize, String> {
    let (next, done) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let failed = Mutex::new(None::<String>);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    let stop = failed.lock().map_or(true, |f| f.is_some());
                    if i >= n || stop {
                        return;
                    }
                    match work(i) {
                        Ok(()) => drop(done.fetch_add(1, Ordering::Relaxed)),
                        Err(e) => {
                            if let Ok(mut f) = failed.lock() {
                                f.get_or_insert(e.to_string());
                            }
                        }
                    }
                }
            });
        }
    });
    match failed.into_inner().unwrap_or_else(|e| e.into_inner()) {
        Some(e) => Err(e),
        None => Ok(done.into_inner()),
    }
}

/// The identity the live tests run against (`HBSYNC_LIVE`).
pub fn live_identity() -> Result<String, String> {
    std::env::var("HBSYNC_LIVE")
        .map_err(|_| "set HBSYNC_LIVE to an identity registered with hbsync-register".into())
}

/// A name for one live test run, unique enough to work in a top-level folder of its own.
pub fn run_name(what: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    format!("hbsync-test {what} {now}")
}

/// A file under one of the drive's top-level folders, as [`delete_top_folders`] finds it.
struct Found {
    at: RemoteRef,
    parent: hbsync_types::NodeId,
    name: String,
    size: u64,
}

/// Deletes for good without a trace (`hard-delete-batch`: no tombstone, not to Recently
/// deleted) the top-level folders whose name `pick` chooses, with everything under them, of
/// those the feed shows from `from` on (`None`: all of it). With `dry`, deletes nothing.
/// Returns each folder's name, its files and their bytes.
pub fn delete_top_folders(
    remote: &dyn Remote,
    from: Option<&str>,
    pick: impl Fn(&str) -> bool,
    dry: bool,
) -> Result<Vec<(String, usize, u64)>, RemoteError> {
    use hbsync_remote::RemoteBody;
    use hbsync_types::NodeId;
    use std::collections::HashMap;
    // The nodes in the tree or in Recently deleted, by node id (a tombstone has none).
    let mut nodes: HashMap<NodeId, Found> = HashMap::new();
    let mut cursor = from.map(str::to_owned);
    loop {
        let page = remote.changes(cursor.as_deref(), 500)?;
        for h in page.entries {
            let at = h.at();
            if let RemoteBody::Live { node, content, .. } = h.body {
                nodes.insert(
                    node,
                    Found {
                        at,
                        parent: h.parent,
                        name: content.name,
                        size: content.size.unwrap_or(0),
                    },
                );
            }
        }
        cursor = Some(page.cursor);
        if !page.has_more {
            break;
        }
    }
    // Each node's top-level folder, where its line up to the root is in what was read.
    let top = |mut node: NodeId| {
        for _ in 0..10_000 {
            let found = nodes.get(&node)?;
            if found.parent == NodeId::ROOT {
                return Some(node);
            }
            node = found.parent;
        }
        None // a loop of parents: not a tree
    };
    let mut folders: HashMap<NodeId, (String, usize, u64)> = HashMap::new();
    let mut doomed = Vec::new();
    for (node, found) in &nodes {
        let Some(t) = top(*node) else { continue };
        let name = &nodes[&t].name;
        if !pick(name) {
            continue;
        }
        let entry = folders.entry(t).or_insert((name.clone(), 0, 0));
        entry.1 += 1;
        entry.2 += found.size;
        doomed.push(found);
    }
    if !dry {
        // A test drive's own folders, live: unchecked.
        let mut asked: Vec<Doomed> = doomed.iter().map(|f| Doomed::unchecked(f.at)).collect();
        // A request that failed after others went: the answers before it still count.
        let mut stopped = None;
        // Written since it was read: once more at its version now.
        for round in 0..2 {
            let mut again = Vec::new();
            let answered = remote.delete_for_good(&asked)?;
            stopped = stopped.or(answered.stopped);
            for (at, answer) in asked.iter().zip(answered.answers) {
                match answer {
                    Ok(()) | Err(RemoteError::NotFound) => {}
                    Err(RemoteError::VersionMismatch) if round == 0 => {
                        again.push(Doomed::unchecked(remote.header(at.at().file_id)?.at()))
                    }
                    Err(e) => return Err(e),
                }
            }
            if again.is_empty() {
                break;
            }
            asked = again;
        }
        if let Some(e) = stopped {
            return Err(e);
        }
    }
    let mut folders: Vec<_> = folders.into_values().collect();
    folders.sort();
    Ok(folders)
}

/// The tombstones the feed shows from `from` on (`None`: the whole drive), each at the version
/// its row has: what a delete left (a removal, a live or removed row, is never one).
pub fn tombstones(remote: &dyn Remote, from: Option<&str>) -> Result<Vec<Doomed>, RemoteError> {
    // A file's last row tells what it is now (a page read during a write may show it twice).
    let mut found = std::collections::HashMap::new();
    let mut cursor = from.map(str::to_owned);
    loop {
        let page = remote.change_refs(cursor.as_deref(), 1000)?;
        for r in page.refs {
            match r.deleted {
                true => found.insert(r.file_id, r.version),
                false => found.remove(&r.file_id),
            };
        }
        cursor = Some(page.cursor);
        if !page.has_more {
            break;
        }
    }
    Ok(found
        .into_iter()
        .map(|(file_id, version)| Doomed::unchecked(RemoteRef { file_id, version }))
        .collect())
}

/// What [`delete_tombstones`] did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Swept {
    /// Deleted for good.
    pub deleted: usize,
    /// Not there (deleted for good meanwhile).
    pub gone: usize,
    /// Written since the feed showed it: left as it is.
    pub written: usize,
}

/// Deletes `tombstones` for good (`hard-delete-batch`, [`hbsync_remote::BATCH_FILES`] a
/// request), each only at its version. A file any other answer refuses, or a request that
/// fails, stops it with that error.
pub fn delete_tombstones(remote: &dyn Remote, tombstones: &[Doomed]) -> Result<Swept, RemoteError> {
    let mut swept = Swept::default();
    for answer in remote.delete_for_good(tombstones)?.into_answers()? {
        match answer {
            Ok(()) => swept.deleted += 1,
            Err(RemoteError::NotFound) => swept.gone += 1,
            Err(RemoteError::VersionMismatch) => swept.written += 1,
            Err(e) => return Err(e),
        }
    }
    Ok(swept)
}

/// The run folders this process holds ([`RunFolder`]): the last one out clears the tombstones
/// written since the first was made.
struct Runs {
    held: usize,
    /// The feed's end when the first run held was made (kept after, to read on from).
    since: Option<String>,
}

static RUNS: Mutex<Runs> = Mutex::new(Runs {
    held: 0,
    since: None,
});

fn runs() -> std::sync::MutexGuard<'static, Runs> {
    RUNS.lock().unwrap_or_else(|e| e.into_inner())
}

/// A live test's run folder on the drive (`name`, a top-level folder): deleted for good with
/// everything in it when the guard is dropped, also when the test panics, so a run that ends
/// early leaves nothing for the next whole-drive sync to take down. Made before the test writes
/// anything: it finds the folder in the feed from where the feed stood then.
///
/// The tombstones a run leaves (its deletes, emptying Recently deleted) would stay on the drive
/// for the 30 days a client keeps them. The last guard of the process to go deletes for good
/// every tombstone written since the first was made: the live tests of one binary run in
/// parallel, so no guard clears them while another run may still read its own. A guard made
/// meanwhile waits for that. Another process's run at the same time (runs on one drive are
/// serialized by a lock, but a daemon or a benchmark may write) may lose its tombstones too:
/// harmless on a test drive, where each is announced as deleted for good.
pub struct RunFolder {
    remote: hbsync_homebase::drive::HomebaseRemote,
    name: String,
    from: String,
}

impl RunFolder {
    /// The guard of the run folder `name`, on its own connection to the live identity.
    pub fn new(name: &str) -> Result<RunFolder, Box<dyn std::error::Error>> {
        let remote =
            hbsync_homebase::drive::HomebaseRemote::connect(saved_client(&live_identity()?)?)?;
        // After the clearing of runs that ended, if one is going on.
        let mut runs = runs();
        let from = hbsync_remote::feed_end(&remote, runs.since.as_deref())?;
        if runs.held == 0 {
            runs.since = Some(from.clone());
        }
        runs.held += 1;
        Ok(RunFolder {
            remote,
            name: name.to_owned(),
            from,
        })
    }
}

impl std::fmt::Debug for RunFolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunFolder")
            .field("name", &self.name)
            .finish()
    }
}

impl Drop for RunFolder {
    fn drop(&mut self) {
        let began = std::time::Instant::now();
        let name = self.name.clone();
        if let Err(e) = delete_top_folders(&self.remote, Some(&self.from), |n| n == name, false) {
            eprintln!("the run folder {:?} is left on the drive: {e}", self.name);
        }
        let mut runs = runs();
        runs.held -= 1;
        // Never the whole drive (`None`): a run held was made at a point of the feed.
        let (0, Some(since)) = (runs.held, runs.since.clone()) else {
            return;
        };
        // The feed shows the last writes once its hold-back has passed.
        std::thread::sleep(self.remote.hold_back().saturating_sub(began.elapsed()));
        let cleared = tombstones(&self.remote, Some(&since))
            .and_then(|t| delete_tombstones(&self.remote, &t));
        if let Err(e) = cleared {
            eprintln!("some of the runs' tombstones may be left on the drive: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use hbsync_remote::contract::{doomed, folder};
    use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
    use hbsync_remote::{NodeMeta, Remote};
    use hbsync_types::NodeId;

    /// The picked top-level folder goes for good with everything under it, Recently deleted
    /// included; the rest of the drive stays, and a dry run deletes nothing.
    #[test]
    fn only_the_picked_folders_go_for_good() -> Result<(), Box<dyn std::error::Error>> {
        let remote = FakeRemote::new(FakeServer::new(ServerConfig::default()));
        let run = folder("hbsync-test x 1", NodeId::ROOT);
        let keep = folder("Documents", NodeId::ROOT);
        let sub = folder("sub", run.node);
        let inside = folder("inner", sub.node);
        let removed = folder("gone", run.node);
        for meta in [&run, &keep, &sub, &inside, &removed] {
            remote.create(meta, None)?;
        }
        remote.create(&folder("mine", keep.node), None)?;
        // `gone` in Recently deleted.
        let header = remote
            .server()
            .live()
            .find(|h| matches!(&h.body, hbsync_remote::RemoteBody::Live { node, .. } if *node == removed.node))
            .cloned()
            .ok_or("gone")?;
        remote.update(
            &hbsync_types::Target {
                at: hbsync_types::RemoteRef {
                    file_id: header.file_id,
                    version: header.version,
                },
                token: header.token.clone(),
            },
            &NodeMeta {
                removed: true,
                ..removed.clone()
            },
            None,
        )?;
        let picked = |n: &str| n.starts_with("hbsync-test ");
        let listed = super::delete_top_folders(&remote, None, picked, true)?;
        assert_eq!(listed, vec![("hbsync-test x 1".to_owned(), 4, 0)]);
        assert_eq!(
            remote.server().live().count(),
            5,
            "a dry run deletes nothing"
        );
        super::delete_top_folders(&remote, None, picked, false)?;
        let mut left = remote.server().live_names();
        left.sort();
        assert_eq!(left, ["Documents", "mine"]);
        assert!(super::delete_top_folders(&remote, None, picked, true)?.is_empty());
        Ok(())
    }

    /// Only tombstones go, from the cursor given on, each at its version: never a live row,
    /// removed or not, nor a tombstone from before.
    #[test]
    fn only_tombstones_go_for_good() -> Result<(), Box<dyn std::error::Error>> {
        use hbsync_remote::feed_end;
        use std::collections::HashSet;
        let remote = FakeRemote::new(FakeServer::new(ServerConfig::default()));
        let mut made = Vec::new();
        for name in ["old", "a", "b", "kept", "removed"] {
            made.push(remote.create(&folder(name, NodeId::ROOT), None)?);
        }
        remote.delete(&doomed(&[made[0].at()]))?.into_answers()?;
        let from = feed_end(&remote, None)?;
        remote
            .delete(&doomed(&[made[1].at(), made[2].at()]))?
            .into_answers()?;
        remote
            .set_removed(&[made[4].target.clone()], true)?
            .into_answers()?;

        let found = super::tombstones(&remote, Some(&from))?;
        let ids: HashSet<_> = found.iter().map(|t| t.at().file_id).collect();
        assert_eq!(ids, made[1..3].iter().map(|t| t.at().file_id).collect());
        assert_eq!(
            super::tombstones(&remote, None)?.len(),
            3,
            "the old one too"
        );

        let swept = super::delete_tombstones(&remote, &found)?;
        assert_eq!(
            swept,
            super::Swept {
                deleted: 2,
                ..Default::default()
            }
        );
        assert_eq!(super::tombstones(&remote, Some(&from))?, []);
        assert_eq!(
            super::tombstones(&remote, None)?.len(),
            1,
            "the old one stays"
        );
        assert_eq!(remote.server().live_names(), ["kept"]);
        assert_eq!(remote.server().removed().count(), 1, "a removed row stays");
        // Gone meanwhile: counted, not an error.
        let again = super::delete_tombstones(&remote, &found)?;
        assert_eq!(
            again,
            super::Swept {
                gone: 2,
                ..Default::default()
            }
        );
        Ok(())
    }
}
