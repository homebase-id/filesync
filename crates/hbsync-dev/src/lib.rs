//! Development tools around the sync engine and a live Homebase identity:
//! - `hbsync-register <identity>`: registers the app and authorizes this machine;
//! - `hbsync-sync <identity> <folder>`: syncs a folder with the identity's Filesync drive.
//!
//! Not the product: the daemon, its configuration and the keychain are roadmap Layer 6.

use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use hbsync_homebase::{Client, Credentials};
use hbsync_remote::{Remote, RemoteError};

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
    file_id: hbsync_types::RemoteFileId,
    version: hbsync_types::VersionTag,
    parent: hbsync_types::NodeId,
    name: String,
    size: u64,
}

/// Deletes for good (not to Recently deleted) the top-level folders whose name `pick` chooses,
/// with everything under them, of those the feed shows from `from` on (`None`: all of it).
/// With `dry`, deletes nothing. Returns each folder's name, its files and their bytes.
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
            if let RemoteBody::Live { node, content, .. } = h.body {
                nodes.insert(
                    node,
                    Found {
                        file_id: h.file_id,
                        version: h.version,
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
        for found in doomed {
            match remote.delete(found.file_id, found.version) {
                Ok(()) | Err(RemoteError::NotFound) => {}
                // Written since it was read: once more at its version now.
                Err(RemoteError::VersionMismatch) => {
                    let h = remote.header(found.file_id)?;
                    if !h.is_deleted() {
                        match remote.delete(found.file_id, h.version) {
                            Ok(()) | Err(RemoteError::NotFound) => {}
                            Err(e) => return Err(e),
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
    let mut folders: Vec<_> = folders.into_values().collect();
    folders.sort();
    Ok(folders)
}

/// A live test's run folder on the drive (`name`, a top-level folder): deleted for good with
/// everything in it when the guard is dropped, also when the test panics, so a run that ends
/// early leaves nothing for the next whole-drive sync to take down. Made before the test writes
/// anything: it finds the folder in the feed from where the feed stood then.
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
        let from = hbsync_remote::contract::feed_end(&remote, None)?;
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
        let name = self.name.clone();
        if let Err(e) = delete_top_folders(&self.remote, Some(&self.from), |n| n == name, false) {
            eprintln!("the run folder {:?} is left on the drive: {e}", self.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use hbsync_remote::contract::folder;
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
                changed_at: None,
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
}
