//! The product end to end through a live identity: two "computers", each with its own
//! `HBSYNC_HOME`, set up with `hbsync setup` and running `hbsyncd`, controlled through `hbsync`:
//! `HBSYNC_LIVE=frodo.dotyou.cloud cargo test -p hbsync --test live_daemon -- --ignored`.
//!
//! Needs the credentials `hbsync-register` saves (copied into each home as a credentials file),
//! and those of a second client (`hbsync-register <identity> --as b`) for the second computer:
//! the notifications leave out a client's own writes (`excludeOwnWrites`), so two computers on
//! one token wouldn't hear each other.
//! Each computer ignores everything on the drive except the test's own top-level folder, which
//! the test deletes at the end.

use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::{Duration, Instant};

use common::Res;

const PATIENCE: Duration = Duration::from_secs(90);

mod common;

use common::{Home, text};

struct Computer {
    home: Home,
    folder: PathBuf,
}

impl Computer {
    /// A computer with the credentials of `client` (`hbsync-register --as <client>`; `None`:
    /// the default one).
    fn set_up(identity: &str, run: &str, client: Option<&str>) -> Res<Computer> {
        let home = Home::new()?;
        let credentials = home.path().join("credentials");
        std::fs::create_dir_all(&credentials)?;
        let saved = hbsync_dev::credentials_or_hint(identity, client)?;
        std::fs::copy(saved, credentials.join(format!("{identity}.json")))?;
        let folder = home.path().join("Homebase");
        std::fs::create_dir_all(folder.join(".hbsync"))?;
        // Only the test's folder: nothing else on the drive is downloaded.
        std::fs::write(
            folder.join(".hbsync").join("ignore"),
            format!("/*\n!/{run}/\n"),
        )?;
        let computer = Computer {
            home,
            folder: hbsync_platform::winpath::plain(&folder.canonicalize()?),
        };
        let path = computer.folder.to_str().ok_or("path")?.to_owned();
        let out = computer.hbsync(&["setup", identity, "--folder", &path])?;
        if !out.status.success() {
            return Err(format!("setup failed: {}", text(&out)).into());
        }
        Ok(computer)
    }

    fn hbsync(&self, args: &[&str]) -> std::io::Result<Output> {
        self.home.hbsync(args)
    }

    fn ok(&self, args: &[&str]) -> Res<String> {
        self.home.ok(args)
    }

    /// The main folder's status.
    fn status(&self) -> Res<serde_json::Value> {
        let status: serde_json::Value = serde_json::from_str(&self.ok(&["status", "--json"])?)?;
        Ok(status["folders"][0].clone())
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
                return Err(format!(
                    "timed out waiting until {what}\n{status}\nits log:\n{}",
                    tail(&self.log())
                )
                .into());
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

/// The last lines of a log.
fn tail(log: &str) -> String {
    let lines: Vec<&str> = log.lines().collect();
    lines[lines.len().saturating_sub(40)..].join("\n")
}

fn exists(p: &Path) -> Res<bool> {
    Ok(p.exists())
}

#[test]
#[ignore = "needs a live identity: HBSYNC_LIVE"]
fn two_computers_sync_through_the_daemon() -> Res<()> {
    let identity = hbsync_dev::live_identity()?;
    let run = hbsync_dev::run_name("daemon");
    let a = Computer::set_up(&identity, &run, None)?;
    // A client of its own, as another computer has: it hears a's writes.
    let b = Computer::set_up(&identity, &run, Some(hbsync_dev::OTHER_CLIENT))?;
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

    // Recently deleted has the folder, and restoring it brings it back on both.
    let many = rel("many");
    a.wait("a lists many/ in Recently deleted", |a| {
        Ok(a.ok(&["deleted", "-n", "500"])?
            .contains(&format!(" {many}/,")))
    })?;
    let out = a.ok(&["restore", &many])?;
    assert!(out.contains("Restoring"), "{out}");
    for c in [&a, &b] {
        let back = c.wait("many/ is back", |c| {
            Ok(std::fs::read_dir(c.path(&many))
                .map(|d| d.count() == 60)
                .unwrap_or(false))
        });
        if let Err(e) = back {
            return Err(format!("{e}\nits log:\n{}", c.log()).into());
        }
    }
    assert_eq!(std::fs::read_to_string(b.path(&rel("many/f07.txt")))?, "7");

    // Clean up: the test's folder leaves the drive (all of a's synced files: confirmed too).
    a.wait("a settled", |a| a.settled())?;
    std::fs::remove_dir_all(a.path(&run))?;
    a.wait("a holds deleting everything", |a| {
        Ok(a.status()?["held_deletes"].is_object())
    })?;
    a.ok(&["confirm-deletes", "--yes"])?;
    // For b that is every file it syncs: held there too, as it should be.
    let held = b.wait("b holds losing everything", |b| {
        Ok(b.status()?["held_deletes"].is_object())
    });
    if let Err(e) = held {
        return Err(format!(
            "{e}\na's log:\n{}\nb's log:\n{}",
            tail(&a.log()),
            tail(&b.log())
        )
        .into());
    }
    assert!(b.path(&rel("hello.txt")).exists());
    b.ok(&["confirm-deletes", "--yes"])?;
    b.wait("b lost the test folder", |b| Ok(!b.path(&run).exists()))?;
    // And off Recently deleted (the test's folder, with everything in it).
    a.wait("a lists the test folder in Recently deleted", |a| {
        Ok(a.ok(&["deleted", "-n", "500"])?
            .contains(&format!(" {run}/,")))
    })?;
    a.ok(&["empty-deleted", "--yes", &run])?;
    a.wait("the test folder left Recently deleted", |a| {
        Ok(!a
            .ok(&["deleted", "-n", "500"])?
            .contains(&format!(" {run}")))
    })?;
    a.ok(&["stop"])?;
    b.ok(&["stop"])?;
    Ok(())
}
