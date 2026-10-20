//! The client harness shared by the engine's integration tests: [`TestClient`] on a shared
//! fake server.

// Each test file uses its own subset.
#![allow(dead_code)]

use std::ops::{Deref, DerefMut};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use hbsync_engine::testkit::TestClient;
use hbsync_engine::{EngineConfig, SyncReport};
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};
use hbsync_state::StateDb;
use hbsync_state::testkit::{Commit, Hook, on_commit};

pub(crate) use hbsync_engine::testkit::Res;

pub(crate) fn server() -> Arc<Mutex<FakeServer>> {
    FakeServer::new(ServerConfig::default())
}

pub(crate) struct Client(TestClient<FakeRemote>);

impl Deref for Client {
    type Target = TestClient<FakeRemote>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Client {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Client {
    pub(crate) fn new(server: &Arc<Mutex<FakeServer>>, name: &str) -> Res<Self> {
        Self::with_config(server, name, |_| {})
    }

    pub(crate) fn with_config(
        server: &Arc<Mutex<FakeServer>>,
        name: &str,
        tweak: impl Fn(&mut EngineConfig) + 'static,
    ) -> Res<Self> {
        let remote = FakeRemote::new(server.clone());
        Ok(Client(TestClient::new(remote, name, move |cfg| {
            // No real pause between transient retries: the fake's faults are made up.
            cfg.exec.retry_pause = std::time::Duration::from_millis(1);
            tweak(cfg);
            Ok(())
        })?))
    }

    /// Scan, then sync until idle. The report accumulates every pass.
    pub(crate) fn sync(&mut self) -> Res<SyncReport> {
        self.0.sync_until_idle("2026-10-08", true, || {})
    }

    /// Syncs until idle as the daemon syncs a local change: reading the feed only if the
    /// drive isn't current.
    pub(crate) fn sync_unasked(&mut self) -> Res<SyncReport> {
        self.0.sync_until_idle("2026-10-08", false, || {})
    }

    /// Takes the drive's changes as announced from now on, as when the notifications connect
    /// and the read that follows works.
    pub(crate) fn announced(&self) {
        let drive = self.engine.drive();
        drive.announcing_from(drive.connection());
    }
}

/// The panic that crashes a client.
struct Crash;

/// How much of its state DB a crashed client keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kept {
    /// Every commit: the process crashed.
    All,
    /// Up to its last durable commit, or its `n`-th commit of the sync if later (`None`: no
    /// other): an OS crash or a power cut, which keeps some of the commits since.
    Durable(Option<usize>),
}

/// Syncs `c` until it crashes where `when` says, given the moment and the commit's number in
/// the sync (just before a commit: that operation's DB changes are lost, what it did on disk or
/// on the drive stays), the DB keeping what `kept` says, then starts it again. Returns whether
/// it crashed.
pub(crate) fn crash(
    c: &mut Client,
    kept: Kept,
    when: impl Fn(Commit, usize) -> bool + Send + Sync + 'static,
) -> Res<bool> {
    let power_cut = kept != Kept::All;
    let db = c.db_path();
    // What is on the disk: every sync ends durably.
    let durable = db.with_file_name("durable.db");
    let keep = {
        let (db, durable) = (db.clone(), durable.clone());
        move || {
            let _ = std::fs::remove_file(&durable);
            StateDb::open_account(&db)
                .and_then(|mut db| db.copy_to(&durable))
                .expect("copying the state DB");
        }
    };
    if power_cut {
        keep();
    }
    let commits = std::sync::atomic::AtomicUsize::new(0);
    let hook = Hook(Arc::new(move |at| {
        let n = commits.load(std::sync::atomic::Ordering::SeqCst);
        if when(at, n) {
            std::panic::panic_any(Crash);
        }
        if at == Commit::Before {
            return;
        }
        commits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if power_cut && (at == Commit::Durable || kept == Kept::Durable(Some(n))) {
            keep();
        }
    }));
    on_commit(&db, Some(hook));
    c.restart()?;
    let synced = std::panic::catch_unwind(AssertUnwindSafe(|| c.sync()));
    on_commit(&db, None);
    let crashed = match synced {
        Ok(synced) => {
            synced?;
            false
        }
        Err(e) if e.is::<Crash>() => true,
        Err(e) => std::panic::resume_unwind(e),
    };
    if crashed && power_cut {
        StateDb::open_account(&db)?.restore_from(&durable)?;
    }
    c.restart()?;
    Ok(crashed)
}
