//! The Homebase Filesync command line: sets syncing up and controls the daemon (`hbsyncd`).

use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use hbsync_app::control::{control, running, start, stop};
use hbsync_app::runner::Hints;
use hbsync_app::{
    Config, Control, Paths, State, Status, autostart, control as daemon, setup, words,
};
use hbsync_local::STATE_DIR;

type Res<T> = hbsync_app::control::Result<T>;

#[derive(Parser)]
#[command(
    name = "hbsync",
    version = hbsync_app::VERSION,
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
    /// Stops syncing until `resume` (also across restarts), or for a while.
    Pause {
        /// Resume by itself after this long: 1h, 8h, 24h, 30m...
        #[arg(long = "for", value_name = "DURATION", value_parser = parse_duration)]
        for_: Option<u64>,
    },
    Resume,
    /// Shows what was synced lately, newest first.
    Recent {
        /// How many entries.
        #[arg(short = 'n', default_value_t = 20)]
        limit: usize,
    },
    /// Shows Recently deleted, newest first: what can still be restored.
    Deleted {
        /// How many items.
        #[arg(short = 'n', default_value_t = 50)]
        limit: usize,
    },
    /// Restores items of Recently deleted (a folder with what was deleted with it).
    Restore {
        /// Paths as `hbsync deleted` shows them, or node ids.
        #[arg(required = true)]
        items: Vec<String>,
    },
    /// Deletes everything in Recently deleted for good.
    EmptyDeleted {
        /// Don't ask.
        #[arg(long)]
        yes: bool,
    },
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

/// Says the daemon isn't running: exit status 3.
fn not_running() -> ExitCode {
    println!("The daemon isn't running (`hbsync start` starts it).");
    ExitCode::from(3)
}

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
            let folder = match folder {
                Some(f) => f,
                None => setup::default_folder()?,
            };
            let config = Config::load(&paths.config_file())?.unwrap_or_default();
            let home = hbsync_app::paths::home();
            let mut request =
                match setup::plan(&config, &identity, &folder, existing, home.as_deref()) {
                    Ok(request) => request,
                    Err(refused @ setup::Refusal::NeedsMerge(_)) => {
                        return Err(format!(
                            "{refused}. Add --existing to do that, or pick an empty or new folder \
                         with --folder."
                        )
                        .into());
                    }
                    Err(refused) => return Err(refused.into()),
                };
            request.start = !no_start;
            request.at_logon = !no_autostart;
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
        Command::Status { json } => match daemon::status(&paths)? {
            Some(s) if json => println!("{}", serde_json::to_string_pretty(&s)?),
            Some(s) => print_status(&s),
            None => return Ok(not_running()),
        },
        Command::Sync => {
            control(&paths, Control::SyncNow)?;
            println!("Syncing now.");
        }
        Command::Pause { for_: None } => {
            control(&paths, Control::Pause { until: None })?;
            println!("Paused. `hbsync resume` syncs again.");
        }
        Command::Pause { for_: Some(secs) } => {
            let until = words::now() + secs;
            control(&paths, Control::Pause { until: Some(until) })?;
            println!(
                "Paused; syncing again {}. `hbsync resume` syncs now.",
                words::when(until, words::now())
            );
        }
        Command::Recent { limit } => match daemon::recent(&paths, limit)? {
            Some(list) if list.is_empty() => println!("Nothing synced yet."),
            Some(list) => {
                let now = words::now();
                for s in &list {
                    println!("{:>12}  {}", words::when(s.at, now), words::synced(s));
                }
            }
            None => return Ok(not_running()),
        },
        Command::Resume => {
            control(&paths, Control::Resume)?;
            println!("Resumed.");
        }
        Command::Deleted { limit } => match daemon::deleted(&paths, limit)? {
            Some(list) if list.is_empty() => println!("Recently deleted is empty."),
            Some(list) => {
                let now = words::now();
                for d in &list {
                    println!("{}  {}", d.node.0, words::deleted(d, now));
                }
            }
            None => return Ok(not_running()),
        },
        Command::Restore { items } => {
            let Some(list) = daemon::deleted(&paths, usize::MAX)? else {
                return Ok(not_running());
            };
            let mut nodes = Vec::new();
            for item in &items {
                // Newest first: a path deleted twice restores its latest.
                let found = list.iter().find(|d| {
                    d.path == item.trim_end_matches('/') || d.node.0.to_string() == *item
                });
                let Some(d) = found else {
                    return Err(format!("{item} isn't in Recently deleted").into());
                };
                nodes.push(d.node);
            }
            control(&paths, Control::Restore { nodes })?;
            println!("Restoring; the next sync brings it back.");
        }
        Command::EmptyDeleted { yes } => {
            let Some(list) = daemon::deleted(&paths, usize::MAX)? else {
                return Ok(not_running());
            };
            if list.is_empty() {
                println!("Recently deleted is empty.");
                return Ok(ExitCode::SUCCESS);
            }
            if !yes {
                print!(
                    "Delete the {} items in Recently deleted for good? [y/N] ",
                    list.len()
                );
                std::io::stdout().flush()?;
                let mut answer = String::new();
                std::io::stdin().lock().read_line(&mut answer)?;
                if !matches!(answer.trim(), "y" | "Y" | "yes") {
                    println!("Nothing deleted.");
                    return Ok(ExitCode::SUCCESS);
                }
            }
            control(&paths, Control::EmptyDeleted { nodes: None })?;
            println!("Emptying Recently deleted.");
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
                println!("Filesync no longer starts at logon.");
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
                println!("Filesync no longer starts at logon.");
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
            None if autostart::installed()? => println!("Filesync starts at logon."),
            None => println!("Filesync doesn't start at logon."),
            Some(OnOff::On) => {
                autostart::install(&autostart::logon_program()?)?;
                println!("Filesync starts at logon.");
            }
            Some(OnOff::Off) => {
                autostart::uninstall()?;
                println!("Filesync no longer starts at logon.");
            }
        },
        Command::Log => println!("{}", paths.logs.display()),
    }
    Ok(ExitCode::SUCCESS)
}

fn confirm_deletes(paths: &Paths, yes: bool) -> Res<()> {
    let Some(status) = daemon::status(paths)? else {
        return Err("the daemon isn't running (`hbsync start` starts it)".into());
    };
    let Some(held) = status.held_deletes else {
        println!("No deletes are waiting for confirmation.");
        return Ok(());
    };
    for line in words::held_deletes(&held) {
        println!("{line}");
    }
    for path in &held.examples {
        println!("  {path}");
    }
    if held.more_than_shown() {
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
    if let Some(p) = &s.progress {
        println!("  {}", words::progress(p));
    }
    if let Some(until) = s.paused_until {
        println!("  syncing again {}", when(until));
    }
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
    words::when(at, words::now())
}

/// "90s", "30m", "8h", "2d" (or plain seconds) as seconds.
fn parse_duration(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let (number, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: u64 = number
        .parse()
        .map_err(|_| format!("{s:?}: expected a number and a unit, like 1h or 30m"))?;
    let unit = match unit {
        "" | "s" => 1,
        "m" | "min" => 60,
        "h" => 3600,
        "d" => 86_400,
        other => return Err(format!("{other:?}: use s, m, h or d")),
    };
    n.checked_mul(unit)
        .filter(|&secs| secs > 0)
        .ok_or_else(|| format!("{s:?} isn't a usable duration"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("1h"), Ok(3600));
        assert_eq!(parse_duration("30m"), Ok(1800));
        assert_eq!(parse_duration("90"), Ok(90));
        assert_eq!(parse_duration("2d"), Ok(172_800));
        assert!(parse_duration("0h").is_err());
        assert!(parse_duration("1w").is_err());
        assert!(parse_duration("h").is_err());
    }
}
