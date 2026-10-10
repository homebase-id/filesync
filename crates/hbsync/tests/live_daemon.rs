//! The product end to end through a live identity: two "computers", each with its own
//! `HBSYNC_HOME`, set up with `hbsync setup` and running `hbsyncd`, controlled through `hbsync`:
//! `HBSYNC_LIVE=frodo.dotyou.cloud cargo test -p hbsync --test live_daemon -- --ignored`.
//!
//! Needs the credentials `hbsync-register` saves (copied into each home as a credentials file).
//! Each computer ignores everything on the drive except the test's own top-level folder, which
//! the test deletes at the end.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

type Res<T> = Result<T, Box<dyn std::error::Error>>;

const PATIENCE: Duration = Duration::from_secs(90);

struct Computer {
    home: tempfile::TempDir,
    folder: PathBuf,
}

impl Computer {
    fn set_up(identity: &str, run: &str) -> Res<Computer> {
        let home = tempfile::tempdir()?;
        let credentials = home.path().join("credentials");
        std::fs::create_dir_all(&credentials)?;
        std::fs::copy(
            hbsync_dev::credentials_path(identity)?,
            credentials.join(format!("{identity}.json")),
        )?;
        let folder = home.path().join("Homebase");
        std::fs::create_dir_all(folder.join(".hbsync"))?;
        // Only the test's folder: nothing else on the drive is downloaded.
        std::fs::write(
            folder.join(".hbsync").join("ignore"),
            format!("/*\n!/{run}/\n"),
        )?;
        let computer = Computer {
            home,
            folder: folder.canonicalize()?,
        };
        let path = computer.folder.to_str().ok_or("path")?.to_owned();
        let out = computer.hbsync(&["setup", identity, "--folder", &path])?;
        if !out.status.success() {
            return Err(format!("setup failed: {}", text(&out)).into());
        }
        Ok(computer)
    }

    fn hbsync(&self, args: &[&str]) -> std::io::Result<Output> {
        Command::new(env!("CARGO_BIN_EXE_hbsync"))
            .args(args)
            .env("HBSYNC_HOME", self.home.path())
            .env("HBSYNC_CREDENTIALS", "file")
            .output()
    }

    fn ok(&self, args: &[&str]) -> Res<String> {
        let out = self.hbsync(args)?;
        if !out.status.success() {
            return Err(format!("hbsync {}: {}", args.join(" "), text(&out)).into());
        }
        Ok(text(&out))
    }

    fn status(&self) -> Res<serde_json::Value> {
        Ok(serde_json::from_str(&self.ok(&["status", "--json"])?)?)
    }

    /// The daemon's log so far.
    fn log(&self) -> String {
        let dir = self.ok(&["log"]).unwrap_or_default();
        std::fs::read_dir(dir.trim())
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| std::fs::read_to_string(e.path()).unwrap_or_default())
            .collect()
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.folder.join(rel)
    }

    fn write(&self, rel: &str, data: &str) -> std::io::Result<()> {
        let p = self.path(rel);
        if let Some(dir) = p.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(p, data)
    }

    fn wait(&self, what: &str, mut done: impl FnMut(&Computer) -> Res<bool>) -> Res<()> {
        let until = Instant::now() + PATIENCE;
        while !done(self)? {
            if Instant::now() > until {
                let status = self.ok(&["status"]).unwrap_or_default();
                return Err(format!("timed out waiting until {what}\n{status}").into());
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        Ok(())
    }

    /// Idle with nothing pending.
    fn settled(&self) -> Res<bool> {
        let s = self.status()?;
        Ok(s["state"] == "idle" && s["pending"] == 0)
    }
}

impl Drop for Computer {
    fn drop(&mut self) {
        let _ = self.hbsync(&["stop"]);
    }
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn exists(p: &Path) -> Res<bool> {
    Ok(p.exists())
}

#[test]
#[ignore = "needs a live identity: HBSYNC_LIVE"]
fn two_computers_sync_through_the_daemon() -> Res<()> {
    let identity = hbsync_dev::live_identity()?;
    let run = hbsync_dev::run_name("daemon");
    let a = Computer::set_up(&identity, &run)?;
    let b = Computer::set_up(&identity, &run)?;
    let rel = |p: &str| format!("{run}/{p}");

    // A file written on one computer shows up on the other.
    a.write(&rel("hello.txt"), "hello")?;
    if let Err(e) = b.wait("b has hello.txt", |b| exists(&b.path(&rel("hello.txt")))) {
        return Err(format!(
            "{e}\na: {}\na's log:\n{}",
            a.ok(&["status"]).unwrap_or_default(),
            a.log()
        )
        .into());
    }
    assert_eq!(std::fs::read_to_string(b.path(&rel("hello.txt")))?, "hello");

    // Paused, b gets nothing; resumed, it catches up.
    b.ok(&["pause"])?;
    assert_eq!(b.status()?["state"], "paused");
    a.write(&rel("second.txt"), "2")?;
    a.wait("a uploaded second.txt", |a| a.settled())?;
    std::thread::sleep(Duration::from_secs(3));
    assert!(
        !b.path(&rel("second.txt")).exists(),
        "b synced while paused"
    );
    b.ok(&["resume"])?;
    b.wait("b has second.txt", |b| exists(&b.path(&rel("second.txt"))))?;

    // A mass delete on b is held until confirmed, then reaches a.
    for i in 0..60 {
        a.write(&rel(&format!("many/f{i:02}.txt")), &i.to_string())?;
    }
    b.wait("b has all 60", |b| {
        Ok(std::fs::read_dir(b.path(&rel("many")))
            .map(|d| d.count() == 60)
            .unwrap_or(false))
    })?;
    b.wait("b settled", |b| b.settled())?;
    std::fs::remove_dir_all(b.path(&rel("many")))?;
    // 60 files and their folder.
    b.wait("b holds the deletes", |b| {
        Ok(b.status()?["held_deletes"]["here"] == 61)
    })?;
    std::thread::sleep(Duration::from_secs(3));
    assert!(
        a.path(&rel("many/f00.txt")).exists(),
        "held: a still has them"
    );
    let out = b.ok(&["confirm-deletes", "--yes"])?;
    assert!(out.contains("Confirmed"), "{out}");
    // a guards its own files too: when the deletes reach it all at once, it holds them.
    a.wait("a lost many/", |a| {
        if a.status()?["held_deletes"].is_object() {
            a.ok(&["confirm-deletes", "--yes"])?;
        }
        Ok(!a.path(&rel("many")).exists())
    })?;

    // Clean up: the test's folder leaves the drive (all of a's synced files: confirmed too).
    a.wait("a settled", |a| a.settled())?;
    std::fs::remove_dir_all(a.path(&run))?;
    a.wait("a holds deleting everything", |a| {
        Ok(a.status()?["held_deletes"].is_object())
    })?;
    a.ok(&["confirm-deletes", "--yes"])?;
    // For b that is every file it syncs: held there too, as it should be.
    b.wait("b holds losing everything", |b| {
        Ok(b.status()?["held_deletes"].is_object())
    })?;
    assert!(b.path(&rel("hello.txt")).exists());
    b.ok(&["confirm-deletes", "--yes"])?;
    b.wait("b lost the test folder", |b| Ok(!b.path(&run).exists()))?;
    a.ok(&["stop"])?;
    b.ok(&["stop"])?;
    Ok(())
}
