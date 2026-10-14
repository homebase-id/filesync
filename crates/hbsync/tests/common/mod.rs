//! A private `HBSYNC_HOME` for running `hbsync` and `hbsyncd` as processes, shared by the CLI
//! tests and the live daemon test. Its daemon is stopped when it goes.

// Each test file uses its own subset.
#![allow(dead_code)]

use std::path::Path;
use std::process::{Command, Output};

pub type Res<T> = Result<T, Box<dyn std::error::Error>>;

pub struct Home {
    dir: tempfile::TempDir,
}

impl Home {
    pub fn new() -> std::io::Result<Home> {
        Ok(Home {
            dir: tempfile::tempdir()?,
        })
    }

    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    /// `exe` with `args`, in this home, credentials in files.
    pub fn command(&self, exe: &str, args: &[&str]) -> Command {
        let mut c = Command::new(exe);
        c.args(args)
            .env("HBSYNC_HOME", self.path())
            .env("HBSYNC_CREDENTIALS", "file");
        c
    }

    pub fn hbsync(&self, args: &[&str]) -> std::io::Result<Output> {
        self.command(env!("CARGO_BIN_EXE_hbsync"), args).output()
    }

    /// `hbsync` with `args`, which must succeed; what it said.
    pub fn ok(&self, args: &[&str]) -> Res<String> {
        let out = self.hbsync(args)?;
        if !out.status.success() {
            return Err(format!("hbsync {}: {}", args.join(" "), text(&out)).into());
        }
        Ok(text(&out))
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        // Never leave a daemon behind.
        let _ = self.hbsync(&["stop"]);
    }
}

/// What a process wrote, stdout then stderr.
pub fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}
