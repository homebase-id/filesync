//! `hbsync` and `hbsyncd` as processes, in a private `HBSYNC_HOME`. The identity never resolves
//! (`.invalid`), so nothing leaves the machine: the daemon stays connecting.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

type R = Result<(), Box<dyn std::error::Error>>;

const IDENTITY: &str = "hbsync-test.invalid";

mod common;

use common::{Home, text};

impl Home {
    /// A config and credentials as `hbsync setup` leaves them, for an identity that can't be
    /// reached.
    fn set_up(&self) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let folder = self.path().join("Homebase");
        std::fs::create_dir_all(&folder)?;
        // As the config keeps it: without `\\?\` on Windows.
        let folder = hbsync_platform::winpath::plain(&folder.canonicalize()?);
        let paths = hbsync_app::Paths::under(self.path());
        hbsync_engine::root::SyncRoot::create(
            &paths.account_db(&IDENTITY.into()),
            IDENTITY,
            &folder,
        )?;
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

/// A TOML basic string (escapes the backslashes of Windows paths).
fn toml_string(p: &Path) -> String {
    format!("\"{}\"", p.display().to_string().replace('\\', "\\\\"))
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
    let out = home.hbsync(&["recent"])?;
    assert_eq!(out.status.code(), Some(3), "{}", text(&out));
    let out = home.hbsync(&["pause", "--for", "1h"])?;
    assert!(text(&out).contains("isn't running"), "{}", text(&out));
    let out = home.hbsync(&["pause", "--for", "soon"])?;
    assert!(text(&out).contains("1h or 30m"), "{}", text(&out));
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
        if status["folders"][0]["state"] == "offline" || Instant::now() > until {
            break status;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let (account, main) = (&status["accounts"][0], &status["folders"][0]);
    assert_eq!(main["state"], "offline", "{status}");
    assert_eq!(main["identity"], IDENTITY);
    assert_eq!(Path::new(main["path"].as_str().ok_or("path")?), folder);
    assert_eq!(account["identity"], IDENTITY);
    assert!(account["credentials_warning"].is_string(), "{status}");
    let out = home.hbsync(&["status"])?;
    assert!(
        text(&out).contains("Homebase Filesync: Offline"),
        "{}",
        text(&out)
    );
    // Syncing needs the identity; pausing doesn't, and is kept for when it starts.
    let out = home.hbsync(&["sync"])?;
    assert!(!out.status.success());
    assert!(text(&out).contains("not connected"), "{}", text(&out));
    let out = home.hbsync(&["pause"])?;
    assert!(out.status.success(), "{}", text(&out));
    let status: serde_json::Value =
        serde_json::from_slice(&home.hbsync(&["status", "--json"])?.stdout)?;
    assert_eq!(status["folders"][0]["state"], "paused", "{status}");
    let config = std::fs::read_to_string(home.path().join("config.toml"))?;
    assert!(config.contains("paused = true"), "{config}");
    let out = home.hbsync(&["resume"])?;
    assert!(out.status.success(), "{}", text(&out));
    // A timed pause ends by itself, also while the account can't connect: the daemon's timer.
    let out = home.hbsync(&["pause", "--for", "2"])?;
    assert!(out.status.success(), "{}", text(&out));
    let main = || crate::status(&home).map(|s| s["folders"][0].clone());
    let paused = main()?;
    assert_eq!(paused["state"], "paused", "{paused}");
    assert!(paused["paused_until"].is_u64(), "{paused}");
    let until = Instant::now() + Duration::from_secs(30);
    let resumed = loop {
        let folder = main()?;
        if folder["state"] != "paused" || Instant::now() > until {
            break folder;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(resumed["state"], "offline", "{resumed}");
    assert!(resumed["paused_until"].is_null(), "{resumed}");

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

#[test]
fn a_role_folder_is_added_and_removed_without_a_daemon() -> R {
    let home = Home::new()?;
    let main = home.set_up()?;
    let docs = home.path().join("My documents");
    std::fs::create_dir_all(&docs)?;
    let docs_arg = docs.to_string_lossy().into_owned();
    let said = home.ok(&["add", "documents", "--folder", &docs_arg])?;
    assert!(said.contains("hbsync start"), "{said}");
    let config = std::fs::read_to_string(home.path().join("config.toml"))?;
    assert!(config.contains("documents"), "{config}");
    let out = home.hbsync(&["add", "pictures", "--folder", &main.to_string_lossy()])?;
    assert!(!out.status.success(), "the main folder is synced already");
    let out = home.hbsync(&["add", "papers"])?;
    assert!(!out.status.success());
    assert!(text(&out).contains("documents"), "{}", text(&out));

    let said = home.ok(&["remove", "documents"])?;
    assert!(said.contains("no longer synced"), "{said}");
    let config = std::fs::read_to_string(home.path().join("config.toml"))?;
    assert!(!config.contains("documents"), "{config}");
    assert!(home.ok(&["remove", "documents"])?.contains("isn't synced"));
    Ok(())
}

#[test]
fn a_role_folder_is_added_for_an_identity_set_up_only() -> R {
    let home = Home::new()?;
    home.set_up()?;
    let docs = home.path().join("Docs");
    std::fs::create_dir_all(&docs)?;
    let out = home.hbsync(&[
        "add",
        "documents",
        "--identity",
        "typo.example",
        "--folder",
        &docs.to_string_lossy(),
    ])?;
    assert!(!out.status.success());
    assert!(text(&out).contains("isn't set up"), "{}", text(&out));
    Ok(())
}

/// The daemon's status, as `hbsync status --json` says it.
fn status(home: &Home) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let out = home.hbsync(&["status", "--json"])?;
    assert!(out.status.success(), "{}", text(&out));
    Ok(serde_json::from_slice(&out.stdout)?)
}

/// Waits until the daemon's status shows `want`: its folders' paths and roles, in order.
fn wait_for_folders(home: &Home, want: &[(&Path, Option<&str>)]) -> R {
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let status = status(home)?;
        let shown: Vec<(PathBuf, Option<String>)> = status["folders"]
            .as_array()
            .ok_or("no folders")?
            .iter()
            .map(|f| {
                let path = PathBuf::from(f["path"].as_str().unwrap_or_default());
                (path, f["role"].as_str().map(str::to_owned))
            })
            .collect();
        let wanted: Vec<(PathBuf, Option<String>)> = want
            .iter()
            .map(|(p, r)| (p.to_path_buf(), r.map(str::to_owned)))
            .collect();
        if shown == wanted {
            return Ok(());
        }
        if Instant::now() > until {
            return Err(format!("shown {shown:?}, wanted {wanted:?}: {status}").into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// While its account waits for the identity, the daemon shows each folder with its role; it
/// keeps them through the account starting over (new credentials), a pause, a folder taken
/// away, and a restart of the daemon.
#[test]
fn the_daemon_keeps_its_folders_through_start_over_and_restart() -> R {
    let home = Home::new()?;
    let main = home.set_up()?;
    let docs = home.path().join("Docs");
    std::fs::create_dir_all(&docs)?;
    let docs = hbsync_platform::winpath::plain(&docs.canonicalize()?);
    home.ok(&["start"])?;
    let said = home.ok(&["add", "documents", "--folder", &docs.to_string_lossy()])?;
    assert!(
        !said.contains("hbsync start"),
        "taken by the daemon: {said}"
    );
    let both = [(main.as_path(), None), (docs.as_path(), Some("documents"))];
    wait_for_folders(&home, &both)?;

    // New credentials: the account starts over with its folders.
    let paths = hbsync_app::Paths::under(home.path());
    hbsync_app::control::start_over(&paths, &IDENTITY.into()).map_err(|e| e.to_string())?;
    wait_for_folders(&home, &both)?;

    home.ok(&["pause"])?;
    let paused = status(&home)?;
    let states: Vec<_> = paused["folders"]
        .as_array()
        .ok_or("no folders")?
        .iter()
        .map(|f| f["state"].clone())
        .collect();
    assert!(states.iter().all(|s| s == "paused"), "{paused}");
    home.ok(&["resume"])?;

    home.ok(&["remove", "documents"])?;
    wait_for_folders(&home, &[(main.as_path(), None)])?;

    home.ok(&["stop"])?;
    home.ok(&["start"])?;
    wait_for_folders(&home, &[(main.as_path(), None)])?;
    home.ok(&["stop"])?;
    Ok(())
}

/// Unlinking while a daemon starts (the tray's, say) leaves no daemon syncing: unlink holds the
/// config's lock from before it stops the daemon until it has emptied the config, and a daemon
/// starting waits for that lock. Here the lock is held first: unlink waits, the daemon still
/// runs, and the next one waits too; then, whichever goes first, none is left syncing.
#[test]
fn unlink_while_a_daemon_starts_leaves_none_syncing() -> R {
    let home = Home::new()?;
    home.set_up()?;
    home.ok(&["start"])?;
    // The config's lock, as `ConfigLock` takes it (from another process: this one).
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(home.path().join("config.lock"))?;
    lock.lock()?;
    let quiet = |args: &[&str], exe: &str| {
        home.command(exe, args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
    };
    let mut unlink = quiet(&["unlink"], env!("CARGO_BIN_EXE_hbsync"))?;
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        unlink.try_wait()?.is_none(),
        "unlink didn't wait for the lock"
    );
    assert!(
        home.hbsync(&["status"])?.status.success(),
        "stopped before the lock"
    );
    let config = std::fs::read_to_string(home.path().join("config.toml"))?;
    assert!(config.contains(IDENTITY), "{config}");
    let mut starting = quiet(&[], env!("CARGO_BIN_EXE_hbsyncd"))?;
    std::thread::sleep(Duration::from_millis(200));
    drop(lock);
    let until = Instant::now() + Duration::from_secs(60);
    while unlink.try_wait()?.is_none() || starting.try_wait()?.is_none() {
        assert!(
            Instant::now() < until,
            "unlink or the daemon starting still runs"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(unlink.wait()?.success());
    let out = home.hbsync(&["status"])?;
    assert_eq!(out.status.code(), Some(3), "{}", text(&out));
    let config = std::fs::read_to_string(home.path().join("config.toml"))?;
    assert!(!config.contains(IDENTITY), "{config}");
    Ok(())
}
