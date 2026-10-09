//! Development tool: registers Homebase Filesync on an identity and authorizes this machine,
//! creates the app's circle, and checks access to the sync drive.
//!
//! ```text
//! hbsync-register <identity> [--browser <command>]
//! ```
//!
//! Opens the identity's authorize page in a browser (the owner logs in and approves the app
//! and its drive), keeps the credentials in `$XDG_CONFIG_HOME/hbsync/dev/<identity>.json`
//! (mode 0600), and is safe to run again: a saved registration is reused.

use std::process::Command;
use std::time::Duration;

use hbsync_homebase::{Client, Credentials, app};
use serde_json::{Value, json};

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(identity) = args.first().cloned() else {
        return Err("usage: hbsync-register <identity> [--browser <command>]".into());
    };
    let browser = args
        .iter()
        .position(|a| a == "--browser")
        .and_then(|i| args.get(i + 1))
        .cloned();
    let path = Credentials::dev_path(&identity)?;

    let mut creds = match Credentials::load(&path)? {
        Some(c) => {
            println!("Using the saved registration in {}", path.display());
            c
        }
        None => {
            let device = hbsync_dev::device_name();
            let pending = hbsync_homebase::begin(&identity, &format!("Filesync on {device}"))?;
            println!("Opening the browser to authorize Homebase Filesync on {identity}...");
            println!("If it doesn't open, visit:\n{}\n", pending.authorize_url);
            open_browser(browser.as_deref(), &pending.authorize_url);
            let creds = pending.finish(Duration::from_secs(1800))?;
            creds.save(&path)?;
            println!("Authorized. Credentials saved to {}", path.display());
            creds
        }
    };
    let client = Client::new(&creds)?;

    // The drive, by its slugs (how the client addresses it from now on).
    let drive: Value = client.get(
        &format!(
            "/api/v2/apps/{}/drives/{}",
            app::APP_SLUG,
            app::SYNC_DRIVE.slug
        ),
        &[],
    )?;
    println!("Sync drive: {}", serde_json::to_string_pretty(&drive)?);
    let target = drive
        .get("targetDrive")
        .cloned()
        .unwrap_or_else(|| app::target_drive(&app::SYNC_DRIVE));
    let alias = target["alias"].as_str().unwrap_or_default().to_owned();

    // The app's circle, once.
    if creds.circle_id.is_none() {
        let id: Value = client.post(
            "/api/v2/connections/circles/create",
            &json!({
                "name": app::CIRCLE_NAME,
                "description": app::CIRCLE_DESCRIPTION,
                "emoji": app::CIRCLE_EMOJI,
                "driveGrants": [{
                    "permissionedDrive": {
                        "drive": target,
                        "permission": app::CIRCLE_DRIVE_PERMISSION,
                    }
                }],
                "permissions": { "keys": [] },
            }),
        )?;
        let id: uuid::Uuid = serde_json::from_value(id)?;
        creds.circle_id = Some(id);
        creds.save(&path)?;
        println!("Created the circle '{}' ({id})", app::CIRCLE_NAME);
    } else {
        println!("Circle already created: {:?}", creds.circle_id);
    }

    // Read access: one page of the change feed.
    let page: Value = client.post(
        &format!("/api/v2/drives/{alias}/files/query-batch"),
        &json!({
            "queryParams": {},
            "resultOptionsRequest": {
                "maxRecords": 10,
                "sorting": "anyChangeDate",
                "ordering": "oldestFirst",
                "includeMetadataHeader": true,
            },
        }),
    )?;
    let files = page["searchResults"].as_array().map_or(0, Vec::len);
    println!("Query on the sync drive works: {files} file(s) on the first page.");
    Ok(())
}

fn open_browser(command: Option<&str>, url: &str) {
    let opener = command.unwrap_or(if cfg!(target_os = "macos") {
        "open"
    } else if cfg!(windows) {
        "explorer"
    } else {
        "xdg-open"
    });
    if let Err(e) = Command::new(opener).arg(url).spawn() {
        eprintln!("couldn't start {opener}: {e}");
    }
}
