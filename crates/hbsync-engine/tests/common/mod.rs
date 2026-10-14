//! The client harness shared by the engine's integration tests: [`TestClient`] on a shared
//! fake server.

// Each test file uses its own subset.
#![allow(dead_code)]

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use hbsync_engine::testkit::TestClient;
use hbsync_engine::{EngineConfig, SyncReport};
use hbsync_remote::fake::{FakeRemote, FakeServer, ServerConfig};

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
            tweak(cfg);
            Ok(())
        })?))
    }

    /// Scan, then sync until idle. The report accumulates every pass.
    pub fn sync(&mut self) -> Res<SyncReport> {
        self.0.sync_until_idle("2026-10-08", || {})
    }
}
