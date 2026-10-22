//! The executor and engine end to end: real folders, a real state DB, the fake remote.

use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use hbsync_engine::exec::{CrashPoint, FailPoint};
use hbsync_engine::walk;
use hbsync_remote::fake::FakeRemote;
use hbsync_remote::{
    ChangesPage, NodeMeta, RefsPage, Remote, RemoteHeader, Result as RemoteResult, Upload,
};
use hbsync_state::StateDb;
use hbsync_state::testkit::Commit;
use hbsync_types::{NodeId, RemoteFileId, RemoteRef, Target};

#[path = "../common/mod.rs"]
mod common;

use common::{Client, server};

type R = Result<(), Box<dyn std::error::Error>>;

fn ignoring(rules: &'static str) -> impl Fn(&mut hbsync_engine::EngineConfig) {
    move |cfg| {
        cfg.folder.filter = std::sync::Arc::new(
            hbsync_local::Filter::from_rules(rules, false).expect("valid rules"),
        );
    }
}

/// Whether nothing of what `c` deleted is kept in its `.hbsync`: nothing aside.
fn nothing_kept(c: &Client) -> bool {
    fs::read_dir(c.root.join(".hbsync/aside")).map_or(true, |mut d| d.next().is_none())
}

/// Another app removes what on the drive `doomed` names, folders first: what is in a removed
/// folder and not named is taken along, live.
fn remove_on_drive(
    server: &Arc<Mutex<hbsync_remote::fake::FakeServer>>,
    doomed: &dyn Fn(&str) -> bool,
) -> R {
    let doomed: Vec<_> = {
        let srv = server.lock().map_err(|_| "poisoned")?;
        let mut doomed: Vec<_> = srv
            .live()
            .filter(|h| h.state().is_some_and(|s| doomed(&s.name)))
            .map(|h| (h.file_id, h.version, h.parent))
            .collect();
        doomed.sort_by_key(|(_, _, parent)| !parent.is_root());
        doomed
    };
    let other = FakeRemote::new(server.clone());
    for (file_id, version, _) in doomed {
        let target = Target {
            at: RemoteRef { file_id, version },
            token: Some(hbsync_remote::fake::token_of(file_id)),
        };
        other.set_removed(&[target], true)?;
    }
    server.lock().map_err(|_| "poisoned")?.tick();
    Ok(())
}

mod crash;
mod deleted;
mod filters;
mod guard;
mod outages;
mod pages;
mod refusals;
mod sync;
mod writers;
