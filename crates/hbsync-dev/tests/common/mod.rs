//! The client harness shared by the live tests that sync folders through a live identity.

#![allow(dead_code)]

use std::fs;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;

use hbsync_engine::testkit::{Res, TestClient};
use hbsync_engine::{EngineConfig, SyncReport};
use hbsync_homebase::drive::HomebaseRemote;
use hbsync_local::Filter;

/// A client of the live identity on a temp folder, and the test's folder it syncs (`run`).
pub(crate) struct Client(
    pub(crate) TestClient<Arc<HomebaseRemote>>,
    pub(crate) String,
);

impl Deref for Client {
    type Target = TestClient<Arc<HomebaseRemote>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Client {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// The ignore rules that leave a client only the test's folder `run`: the drive is shared with
/// other tests and with what runs that ended early left behind.
pub(crate) fn only(run: &str) -> String {
    format!("/*\n!/{run}/\n")
}

impl Client {
    /// A client of `identity` that syncs only the folder `run`.
    pub(crate) fn new(identity: &str, name: &str, run: &str) -> Res<Self> {
        let remote = Arc::new(HomebaseRemote::connect(hbsync_dev::saved_client(
            identity,
        )?)?);
        let rules = only(run);
        let configure = move |cfg: &mut EngineConfig| -> Res<()> {
            cfg.guard.allow = true; // the test deletes its whole folder at the end
            // Before the first start; a restart keeps the rules a scenario set.
            let ignore = hbsync_local::layout::ignore_file(&cfg.folder.root);
            if !ignore.exists() {
                fs::write(&ignore, &rules)?;
            }
            cfg.folder.filter =
                Arc::new(Filter::load(&cfg.folder.root, cfg.folder.case_insensitive)?);
            Ok(())
        };
        Ok(Client(
            TestClient::new(remote, name, configure)?,
            run.to_owned(),
        ))
    }

    /// Scan, then sync until idle: nothing may fail or be skipped. A failure names the test's
    /// line that synced, and each step that failed with its error.
    #[track_caller]
    pub(crate) fn sync(&mut self) -> Res<SyncReport> {
        // The feed rows it can't read, as the daemon's account is told them.
        let skipped = Arc::new(std::sync::Mutex::new(Vec::new()));
        let told = Arc::clone(&skipped);
        self.0.engine.drive().tell_skipped(move |rows| {
            told.lock().unwrap_or_else(|e| e.into_inner()).extend(rows);
        });
        // Homebase holds back feed rows of the current millisecond.
        let total = self.0.sync_until_idle(&hbsync_app::today(), true, || {
            std::thread::sleep(Duration::from_millis(20));
        })?;
        if !total.failed.is_empty() {
            let steps: Vec<String> = total
                .failed
                .iter()
                .map(|(op, e)| format!("  {op:?}\n    error: {e}"))
                .collect();
            panic!(
                "{}: {} sync steps failed:\n{}",
                self.name,
                steps.len(),
                steps.join("\n")
            );
        }
        let skipped = std::mem::take(&mut *skipped.lock().unwrap_or_else(|e| e.into_inner()));
        assert!(
            skipped.is_empty(),
            "{}: feed rows skipped {skipped:?}",
            self.name
        );
        Ok(total)
    }
}
