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
use hbsync_state::testkit::{Commit, Hook, on_commit};

pub use hbsync_engine::testkit::Res;

pub fn server() -> Arc<Mutex<FakeServer>> {
    FakeServer::new(ServerConfig::default())
}

pub struct Client(TestClient<FakeRemote>);

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
    pub fn new(server: &Arc<Mutex<FakeServer>>, name: &str) -> Res<Self> {
        Self::with_config(server, name, |_| {})
    }

    pub fn with_config(
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
    pub fn sync(&mut self) -> Res<SyncReport> {
        self.0.sync_until_idle("2026-10-08", || {})
    }
}

/// The panic that crashes a client.
struct Crash;

/// Syncs `c` until it crashes at a commit `when` picks (just before one: that operation's DB
/// changes are lost, what it did on disk or on the drive stays), then starts it again.
/// Returns whether it crashed.
pub fn crash(c: &mut Client, when: impl Fn(Commit) -> bool + Send + Sync + 'static) -> Res<bool> {
    let db = c.db_path();
    let hook = Hook(Arc::new(move |at| {
        if when(at) {
            std::panic::panic_any(Crash);
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
    c.restart()?;
    Ok(crashed)
}
