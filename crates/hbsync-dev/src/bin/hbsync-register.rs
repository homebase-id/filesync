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

use hbsync_app::setup;
use hbsync_homebase::Credentials;

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
    let path = hbsync_dev::credentials_path(&identity)?;

    let mut creds = match Credentials::load(&path)? {
        Some(c) => {
            println!("Using the saved registration in {}", path.display());
            c
        }
        None => {
            let device = hbsync_platform::host::device_name();
            println!("Opening the browser to authorize Homebase Filesync on {identity}...");
            let creds = setup::authorize(&identity, &device, browser.as_deref(), |url| {
                println!("If it doesn't open, visit:\n{url}\n");
            })?;
            creds.save(&path)?;
            println!("Authorized. Credentials saved to {}", path.display());
            creds
        }
    };
    let prepared = setup::prepare(&mut creds)?;
    let drive = prepared.remote.drive();
    println!(
        "Sync drive: alias {}, type {}",
        drive.alias.simple(),
        drive.drive_type.simple()
    );
    match prepared.circle_created {
        Some(id) => {
            creds.save(&path)?;
            println!(
                "Created the circle '{}' ({id})",
                hbsync_homebase::app::CIRCLE_NAME
            );
        }
        None => println!("Circle already created: {:?}", creds.circle_id),
    }
    println!(
        "The sync drive's feed works: {} change(s) on the first page.",
        prepared.feed_entries
    );
    Ok(())
}
