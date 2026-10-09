//! `hbsync` and `hbsyncd` as processes, in a private `HBSYNC_HOME`. The identity never resolves
//! (`.invalid`), so nothing leaves the machine: the daemon stays connecting.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

type R = Result<(), Box<dyn std::error::Error>>;

const IDENTITY: &str = "hbsync-test.invalid";

struct Home {
    dir: tempfile::TempDir,
}

impl Home {
    fn new() -> std::io::Result<Home> {
        Ok(Home {
            dir: tempfile::tempdir()?,
        })
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn command(&self, exe: &str, args: &[&str]) -> Command {
        let mut c = Command::new(exe);
        c.args(args)
            .env("HBSYNC_HOME", self.path())
            .env("HBSYNC_CREDENTIALS", "file");
        c
    }

    fn hbsync(&self, args: &[&str]) -> std::io::Result<Output> {
        self.command(env!("CARGO_BIN_EXE_hbsync"), args).output()
    }

    /// A config and credentials as `hbsync setup` leaves them, for an identity that can't be
    /// reached.
    fn set_up(&self) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let folder = self.path().join("Homebase");
        std::fs::create_dir_all(folder.join(".hbsync"))?;
        let folder = folder.canonicalize()?;
        std::fs::write(
            self.path().join("config.toml"),
            format!(
                "[[roots]]\nidentity = \"{IDENTITY}\"\nfolder = {}\n",
                toml_string(&folder)
            ),
        )?;
        let creds = hbsync_homebase::Credentials {
            identity: IDENTITY.into(),
            client_auth_token: "Y2F0".into(),
            shared_secret: "AAECAwQFBgcICQoLDA0ODw==".into(),
            circle_id: None,
        };
        creds.save(&self.credentials())?;
        Ok(folder)
    }

    fn credentials(&self) -> PathBuf {
        self.path()
            .join("credentials")
            .join(format!("{IDENTITY}.json"))
    }
}

impl Drop for Home {
    fn drop(&mut self) {
        // Never leave a daemon behind.
        let _ = self.hbsync(&["stop"]);
    }
}

/// A TOML basic string (escapes the backslashes of Windows paths).
fn toml_string(p: &Path) -> String {
    format!("\"{}\"", p.display().to_string().replace('\\', "\\\\"))
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn status_without_a_daemon() -> R {
    let home = Home::new()?;
    let out = home.hbsync(&["status"])?;
    assert_eq!(out.status.code(), Some(3), "{}", text(&out));
    assert!(text(&out).contains("isn't running"), "{}", text(&out));
    let out = home.hbsync(&["sync"])?;
    assert!(!out.status.success());
    assert!(text(&out).contains("isn't running"), "{}", text(&out));
    Ok(())
}

#[test]
fn setup_refuses_a_folder_with_files_in_it() -> R {
    let home = Home::new()?;
    let folder = home.path().join("full");
    std::fs::create_dir_all(&folder)?;
    std::fs::write(folder.join("mine.txt"), "mine")?;
    let out = home.hbsync(&[
        "setup",
        IDENTITY,
        "--folder",
        folder.to_str().ok_or("path")?,
    ])?;
    assert!(!out.status.success(), "{}", text(&out));
    assert!(text(&out).contains("--existing"), "{}", text(&out));
    assert!(!home.path().join("config.toml").exists());
    assert!(!folder.join(".hbsync").exists());
    Ok(())
}

#[test]
fn the_daemon_answers_while_offline_and_stops() -> R {
    let home = Home::new()?;
    let folder = home.set_up()?;

    let out = home.hbsync(&["start"])?;
    assert!(out.status.success(), "{}", text(&out));

    // A second daemon has nothing to do: it says so and exits, successfully (a service
    // manager would restart a failed one).
    let out = home.command(env!("CARGO_BIN_EXE_hbsyncd"), &[]).output()?;
    assert!(out.status.success(), "{}", text(&out));
    assert!(text(&out).contains("already running"), "{}", text(&out));

    // It answers while it can't reach the identity, and says why.
    let until = Instant::now() + Duration::from_secs(30);
    let status = loop {
        let out = home.hbsync(&["status", "--json"])?;
        assert!(out.status.success(), "{}", text(&out));
        let status: serde_json::Value = serde_json::from_slice(&out.stdout)?;
        if status["state"] == "offline" || Instant::now() > until {
            break status;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(status["state"], "offline", "{status}");
    assert_eq!(status["identity"], IDENTITY);
    assert_eq!(
        Path::new(status["folder"].as_str().ok_or("folder")?),
        folder
    );
    assert!(status["credentials_warning"].is_string(), "{status}");
    let out = home.hbsync(&["status"])?;
    assert!(text(&out).contains("offline"), "{}", text(&out));
    // Syncing needs the identity; pausing doesn't, and is kept for when it starts.
    let out = home.hbsync(&["sync"])?;
    assert!(!out.status.success());
    assert!(text(&out).contains("not connected"), "{}", text(&out));
    let out = home.hbsync(&["pause"])?;
    assert!(out.status.success(), "{}", text(&out));
    let status: serde_json::Value =
        serde_json::from_slice(&home.hbsync(&["status", "--json"])?.stdout)?;
    assert_eq!(status["state"], "paused", "{status}");
    let config = std::fs::read_to_string(home.path().join("config.toml"))?;
    assert!(config.contains("paused = true"), "{config}");
    let out = home.hbsync(&["resume"])?;
    assert!(out.status.success(), "{}", text(&out));

    let out = home.hbsync(&["stop"])?;
    assert!(out.status.success(), "{}", text(&out));
    assert!(text(&out).contains("Stopped"), "{}", text(&out));
    let out = home.hbsync(&["status"])?;
    assert_eq!(out.status.code(), Some(3), "{}", text(&out));

    // It logged where `hbsync log` says.
    let out = home.hbsync(&["log"])?;
    let logs = PathBuf::from(String::from_utf8_lossy(&out.stdout).trim());
    let logged: String = std::fs::read_dir(&logs)?
        .flatten()
        .map(|e| std::fs::read_to_string(e.path()).unwrap_or_default())
        .collect();
    assert!(logged.contains("daemon"), "{logged}");
    Ok(())
}

#[test]
fn logout_and_unlink_keep_the_files() -> R {
    let home = Home::new()?;
    let folder = home.set_up()?;
    std::fs::write(folder.join("mine.txt"), "mine")?;

    let out = home.hbsync(&["logout"])?;
    assert!(out.status.success(), "{}", text(&out));
    assert!(!home.credentials().exists());
    let out = home.hbsync(&["unlink"])?;
    assert!(out.status.success(), "{}", text(&out));
    assert!(text(&out).contains("no longer synced"), "{}", text(&out));
    assert!(folder.join("mine.txt").exists());
    let config = std::fs::read_to_string(home.path().join("config.toml"))?;
    assert!(!config.contains(IDENTITY), "{config}");
    Ok(())
}
