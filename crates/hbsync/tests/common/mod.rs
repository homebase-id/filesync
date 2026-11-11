//! A private `HBSYNC_HOME` for running `hbsync` and `hbsyncd` as processes, shared by the CLI
//! tests and the live daemon test. Its daemon is stopped when it goes.

// Each test file uses its own subset.
#![allow(dead_code)]

use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

pub(crate) type Res<T> = Result<T, Box<dyn std::error::Error>>;

pub(crate) struct Home {
    dir: tempfile::TempDir,
}

impl Home {
    pub(crate) fn new() -> std::io::Result<Home> {
        Ok(Home {
            dir: tempfile::tempdir()?,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        self.dir.path()
    }

    /// `exe` with `args`, in this home, credentials in files.
    pub(crate) fn command(&self, exe: &str, args: &[&str]) -> Command {
        let mut c = Command::new(exe);
        c.args(args)
            .env("HBSYNC_HOME", self.path())
            .env("HBSYNC_CREDENTIALS", "file");
        c
    }

    pub(crate) fn hbsync(&self, args: &[&str]) -> std::io::Result<Output> {
        self.command(env!("CARGO_BIN_EXE_hbsync"), args).output()
    }

    /// `hbsync` with `args`, which must succeed; what it said.
    pub(crate) fn ok(&self, args: &[&str]) -> Res<String> {
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
pub(crate) fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// The first connection `listener` takes within `within` (made non-blocking), if any; one
/// waiting already is taken also when `within` is zero.
pub(crate) fn accepted(
    listener: &TcpListener,
    within: Duration,
) -> std::io::Result<Option<TcpStream>> {
    listener.set_nonblocking(true)?;
    let until = Instant::now() + within;
    loop {
        match listener.accept() {
            Ok((stream, _)) => return Ok(Some(stream)),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= until {
                    return Ok(None);
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(e) => return Err(e),
        }
    }
}
