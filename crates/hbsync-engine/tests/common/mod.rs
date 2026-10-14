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

/// The engine config tests use: no real pause between transient retries.
pub fn config(root: &std::path::Path, name: &str) -> EngineConfig {
    let mut cfg = EngineConfig::new(root, name);
    cfg.exec.retry_pause = std::time::Duration::from_millis(1);
    cfg
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
        let configure = Box::new(move |cfg: &mut EngineConfig| {
            tweak(cfg);
            Ok(())
        });
        let remote = FakeRemote::new(server.clone());
        Ok(Client(TestClient::new(remote, name, configure)?))
    }

    /// Scan, then sync until idle. The report accumulates every pass.
    pub fn sync(&mut self) -> Res<SyncReport> {
        self.0.sync_until_idle("2026-10-08", || {})
    }

    pub fn read(&self, rel: &str) -> std::io::Result<String> {
        Ok(String::from_utf8_lossy(&self.0.read(rel)?).into_owned())
    }

    /// `(path, content)` for files, `(path/, "")` for folders, sorted; the state folder
    /// excluded.
    pub fn tree(&self) -> Vec<(String, Vec<u8>)> {
        self.0.tree("")
    }

    /// [`Client::tree`] as text: `path = content` for files, `path/` for folders.
    pub fn tree_text(&self) -> Vec<String> {
        self.tree()
            .into_iter()
            .map(|(path, data)| {
                if path.ends_with('/') {
                    path
                } else {
                    format!("{path} = {}", String::from_utf8_lossy(&data))
                }
            })
            .collect()
    }
}
