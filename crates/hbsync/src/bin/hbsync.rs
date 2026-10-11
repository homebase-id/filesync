//! The Homebase Filesync command line: sets syncing up and controls the daemon (`hbsyncd`).

use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use hbsync_app::control::{ask, control, running, start, stop};
use hbsync_app::ipc::{Ask, IpcError, Reply};
use hbsync_app::runner::Hints;
use hbsync_app::{Control, Paths, State, Status, autostart, control as daemon, setup};
use hbsync_local::STATE_DIR;

type Res<T> = hbsync_app::control::Result<T>;

#[derive(Parser)]
#[command(
    name = "hbsync",
    version,
    about = "Homebase Filesync: keeps a folder in sync with your Homebase identity"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Sets syncing up: authorizes this computer on IDENTITY (you approve in the browser),
    /// prepares the folder, and starts the daemon.
    Setup {
        /// Your identity's domain, e.g. anna.example.com.
        identity: String,
        /// The folder to sync [default: Homebase in your home folder].
        #[arg(long)]
        folder: Option<PathBuf>,
        /// Sync a folder that already has files in it (they are merged with the drive).
        #[arg(long)]
        existing: bool,
        /// The browser to open the authorize page with.
        #[arg(long, value_name = "COMMAND")]
        browser: Option<String>,
        /// Don't start the daemon.
        #[arg(long)]
        no_start: bool,
        /// Don't start the daemon at logon.
        #[arg(long)]
        no_autostart: bool,
    },
    /// Shows what the daemon is doing.
    Status {
        /// As JSON.
        #[arg(long)]
        json: bool,
    },
    /// Syncs now.
    Sync,
    /// Stops syncing until `resume` (also across restarts).
    Pause,
    Resume,
    /// Lets through the deletes held for confirmation.
    ConfirmDeletes {
        /// Don't ask.
        #[arg(long)]
        yes: bool,
    },
    /// Starts the daemon.
    Start,
    /// Stops the daemon.
    Stop,
    /// Forgets this computer's credentials. The folder and its files stay.
    Logout,
    /// Stops syncing the folder. Its files stay; setting it up again picks up where it was.
    Unlink,
    /// Starts the daemon at logon (on), or not (off); shows which without an argument.
    Autostart {
        state: Option<OnOff>,
    },
    /// Prints the folder of the daemon's logs.
    Log,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum OnOff {
    On,
    Off,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli.command) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("hbsync: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Exit code of `status` when the daemon isn't running.
const NOT_RUNNING: u8 = 3;

fn run(command: Command) -> Res<ExitCode> {
    let paths = Paths::resolve()?;
    match command {
        Command::Setup {
            identity,
            folder,
            existing,
            browser,
            no_start,
            no_autostart,
        } => {
            let request = setup::SetUp {
                identity: setup::normalize_identity(&identity),
                folder: match folder {
                    Some(f) => std::path::absolute(f)?,
                    None => setup::default_folder()?,
                },
                existing,
                start: !no_start,
                at_logon: !no_autostart,
            };
            let mut authorize = |identity: &str, device: &str| {
                println!("Opening your browser to authorize this computer on {identity}...");
                setup::authorize(identity, device, browser.as_deref(), |url| {
                    println!("If it doesn't open, visit:\n{url}\n");
                })
            };
            setup::set_up(&paths, &request, &mut authorize, &mut |line| {
                println!("{line}")
            })?;
        }
        Command::Status { json } => match ask(&paths, Ask::Status) {
            Ok(Reply::Status(s)) if json => println!("{}", serde_json::to_string_pretty(&s)?),
            Ok(Reply::Status(s)) => print_status(&s),
            Ok(other) => return Err(format!("unexpected answer: {other:?}").into()),
            Err(IpcError::NotRunning) => {
                println!("The daemon isn't running (`hbsync start` starts it).");
                return Ok(ExitCode::from(NOT_RUNNING));
            }
            Err(e) => return Err(e.into()),
        },
        Command::Sync => {
            control(&paths, Control::SyncNow)?;
            println!("Syncing now.");
        }
        Command::Pause => {
            control(&paths, Control::Pause)?;
            println!("Paused. `hbsync resume` syncs again.");
        }
        Command::Resume => {
            control(&paths, Control::Resume)?;
            println!("Resumed.");
        }
        Command::ConfirmDeletes { yes } => confirm_deletes(&paths, yes)?,
        Command::Start => {
            if running(&paths)? {
                println!("The daemon is already running.");
            } else {
                start(&paths)?;
                println!("Started.");
            }
        }
        Command::Stop => {
            if stop(&paths)? {
                println!("Stopped.");
            } else {
                println!("The daemon wasn't running.");
            }
        }
        Command::Logout => {
            let Some(out) = daemon::log_out(&paths)? else {
                println!("Not set up.");
                return Ok(ExitCode::SUCCESS);
            };
            if out.autostart_was_on {
                println!("The daemon no longer starts at logon.");
            }
            if let Some(why) = out.keychain_unreachable {
                println!(
                    "The system keychain couldn't be reached ({why}): if it holds the \
                     credentials, run `hbsync logout` again where it can be (your desktop \
                     session)."
                );
            }
            let root = out.root;
            println!(
                "Logged out of {}. {} and its files stay; `hbsync setup {}` signs in again.",
                root.identity,
                root.folder.display(),
                root.identity
            );
        }
        Command::Unlink => {
            let Some(out) = daemon::unlink(&paths)? else {
                println!("Not set up.");
                return Ok(ExitCode::SUCCESS);
            };
            if out.autostart_was_on {
                println!("The daemon no longer starts at logon.");
            }
            for root in out.roots {
                println!(
                    "{} is no longer synced with {}. Its files stay.",
                    root.folder.display(),
                    root.identity
                );
            }
        }
        Command::Autostart { state } => match state {
            None if autostart::installed()? => println!("The daemon starts at logon."),
            None => println!("The daemon doesn't start at logon."),
            Some(OnOff::On) => {
                autostart::install(&autostart::daemon_exe()?)?;
                println!("The daemon starts at logon.");
            }
            Some(OnOff::Off) => {
                autostart::uninstall()?;
                println!("The daemon no longer starts at logon.");
            }
        },
        Command::Log => println!("{}", paths.logs.display()),
    }
    Ok(ExitCode::SUCCESS)
}

fn confirm_deletes(paths: &Paths, yes: bool) -> Res<()> {
    let status = match ask(paths, Ask::Status)? {
        Reply::Status(s) => s,
        other => return Err(format!("unexpected answer: {other:?}").into()),
    };
    let Some(held) = status.held_deletes else {
        println!("No deletes are waiting for confirmation.");
        return Ok(());
    };
    if held.here > 0 {
        println!(
            "{} items deleted on this computer will be deleted from {} and your other devices.",
            held.here, status.identity
        );
    }
    if held.remotely > 0 {
        println!(
            "{} items deleted elsewhere will be moved to this computer's trash.",
            held.remotely
        );
    }
    for path in &held.examples {
        println!("  {path}");
    }
    if (held.examples.len() as u64) < held.here + held.remotely {
        println!("  ...");
    }
    if !yes {
        print!("Go ahead? [y/N] ");
        std::io::stdout().flush()?;
        let mut answer = String::new();
        std::io::stdin().lock().read_line(&mut answer)?;
        if !matches!(answer.trim(), "y" | "Y" | "yes") {
            println!("Nothing deleted; they stay held.");
            return Ok(());
        }
    }
    // Exactly these: any delete since is held again, for another look.
    control(paths, Control::ConfirmDeletes { token: held.token })?;
    println!("Confirmed.");
    Ok(())
}

fn print_status(s: &Status) {
    let state = match s.state {
        State::Starting => "starting",
        State::Syncing => "syncing",
        State::Idle if s.pending == 0 => "up to date",
        State::Idle => "idle",
        State::Paused => "paused",
        State::Offline => "offline",
        State::Failing => "failing",
        State::Denied => "refused by the identity",
        State::Stopped => "stopped",
    };
    println!("Homebase Filesync: {state}");
    println!("  {} <-> {}", s.identity, s.folder.display());
    if let Some(e) = &s.error {
        println!("  problem: {e}");
    }
    if s.state == State::Denied {
        println!(
            "  authorize this computer again: hbsync setup {}",
            s.identity
        );
    }
    if let Some(at) = s.retry_at {
        println!("  next try {}", when(at));
    }
    if let Some(last) = &s.last_sync {
        println!(
            "  last sync {} ({} done, {:.1} s)",
            when(last.at),
            last.done,
            last.millis as f64 / 1000.0
        );
    }
    if s.pending > 0 {
        println!("  {} items not synced yet", s.pending);
    }
    if let Some(h) = &s.held_deletes {
        println!(
            "  deletes held: {} here, {} elsewhere; `hbsync confirm-deletes` lets them through",
            h.here, h.remotely
        );
    }
    match &s.hints {
        Hints::Off | Hints::Connected => {}
        Hints::Connecting => println!("  notifications: connecting"),
        Hints::Lost(why) => println!("  notifications: lost ({why}), reconnecting"),
        Hints::Refused(why) => println!("  notifications: refused ({why}); syncing every minute"),
    }
    if let Some(why) = &s.watcher_error {
        println!("  no live watcher ({why}): the folder is rescanned every minute");
    }
    if s.rules > 0 {
        println!(
            "  {} ignore {} in {}/{}",
            s.rules,
            if s.rules == 1 { "rule" } else { "rules" },
            STATE_DIR,
            hbsync_local::IGNORE_FILE
        );
    }
    if let Some(w) = &s.credentials_warning {
        println!("  credentials: {w}");
    }
    if !s.recent.is_empty() {
        println!("  recently:");
        for note in s.recent.iter().rev().take(10) {
            println!("    {} {}", when(note.at), note.text);
        }
    }
}

/// `at` (Unix seconds) relative to now: "12 s ago", "in 3 min".
fn when(at: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let (secs, ago) = if at <= now {
        (now - at, true)
    } else {
        (at - now, false)
    };
    let span = match secs {
        0..60 => format!("{secs} s"),
        60..3600 => format!("{} min", secs / 60),
        3600..86_400 => format!("{} h", secs / 3600),
        _ => format!("{} days", secs / 86_400),
    };
    if ago {
        format!("{span} ago")
    } else {
        format!("in {span}")
    }
}
