//! The Homebase Filesync command line: sets syncing up and controls the daemon (`hbsyncd`).

use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use hbsync_app::control::{running, start, stop};
use hbsync_app::daemon::UNANNOUNCED_READ;
use hbsync_app::folders::{self, Taken};
use hbsync_app::protocol::{DaemonControl, DaemonStatus, DeletedItem, FolderStatus, Hints, Pause};
use hbsync_app::{Config, Paths, State, autostart, control as daemon, setup, words};
use hbsync_local::STATE_DIR;
use hbsync_types::Role;

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
    /// Syncs one of your own folders where this computer keeps it (documents, pictures,
    /// music, videos, downloads) with the drive's folder of that kind: joined if another device
    /// syncs it already (identical files are adopted, not copied), made if not.
    Add {
        #[arg(value_parser = parse_role)]
        role: Role,
        /// The identity to sync it with [default: the one set up].
        #[arg(long)]
        identity: Option<String>,
        /// Where this computer keeps it, if not where the OS says.
        #[arg(long)]
        folder: Option<PathBuf>,
    },
    /// Stops syncing one of your own folders. Its files stay; adding it again picks up where it
    /// was.
    Remove {
        #[arg(value_parser = parse_role)]
        role: Role,
        /// The identity it syncs with [default: the one set up].
        #[arg(long)]
        identity: Option<String>,
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
        /// How many deletes (a deleted folder is one, with everything in it).
        #[arg(short = 'n', default_value_t = 50)]
        limit: usize,
    },
    /// Restores items of Recently deleted (a folder with what was deleted with it).
    Restore {
        /// Paths as `hbsync deleted` shows them, or node ids.
        #[arg(required = true)]
        items: Vec<String>,
    },
    /// Deletes items of Recently deleted for good (each with what was in it as shown), or
    /// everything in it.
    EmptyDeleted {
        /// Paths as `hbsync deleted` shows them, or node ids [default: everything].
        items: Vec<String>,
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
            daemon::daemon(&paths, DaemonControl::SyncNow)?;
            println!("Syncing now.");
        }
        Command::Pause { for_: None } => {
            daemon::daemon(&paths, DaemonControl::Pause(Pause::UNTIL_RESUMED))?;
            println!("Paused. `hbsync resume` syncs again.");
        }
        Command::Pause { for_: Some(secs) } => {
            let until = words::now() + secs;
            daemon::daemon(&paths, DaemonControl::Pause(Pause::ending_at(until)))?;
            println!(
                "Paused; syncing again {}. `hbsync resume` syncs now.",
                words::when(until, words::now())
            );
        }
        Command::Recent { limit } => match daemon::recent(&paths, limit)? {
            Some(list) if list.is_empty() => println!("Nothing synced yet."),
            Some(list) => {
                let now = words::now();
                for item in &list {
                    let s = &item.synced;
                    // Items of another folder than the main one say which.
                    let folder = if item.main {
                        String::new()
                    } else {
                        format!(" (in {})", item.folder.display())
                    };
                    println!(
                        "{:>12}  {}{folder}",
                        words::when(s.at, now),
                        words::synced(s)
                    );
                }
            }
            None => return Ok(not_running()),
        },
        Command::Resume => {
            daemon::daemon(&paths, DaemonControl::Resume)?;
            println!("Resumed.");
        }
        Command::Deleted { limit } => match daemon::deleted(&paths, limit)?.map(|l| l.items) {
            Some(list) if list.is_empty() => println!("Recently deleted is empty."),
            Some(list) => {
                // Each item deleted on its own, with how many items are in it.
                for (item, what) in list.iter().zip(described(&list)) {
                    println!("{}  {what}", item.deleted.node.0);
                }
            }
            None => return Ok(not_running()),
        },
        Command::Restore { items } => {
            let Some(found) = daemon::find_deleted(&paths, items)? else {
                return Ok(not_running());
            };
            daemon::restore(&paths, &found)?;
            println!("Restoring; the next sync brings it back.");
        }
        Command::EmptyDeleted { items, yes } if !items.is_empty() => {
            let Some(found) = daemon::find_deleted(&paths, items.clone())? else {
                return Ok(not_running());
            };
            // What was found, as `hbsync deleted` shows it: the user's words may name another.
            let question = format!(
                "In Recently deleted:\n  {}\nDelete {} for good?",
                described(&found).join("\n  "),
                if found.len() == 1 { "it" } else { "them" }
            );
            if !yes && !ask_yes(&question)? {
                println!("Nothing deleted.");
                return Ok(ExitCode::SUCCESS);
            }
            daemon::empty(&paths, &found)?;
            let staying: Vec<DeletedItem> =
                found.iter().filter(|i| i.deleted.stays).cloned().collect();
            println!("{}", emptied(staying.len()));
            for what in described(&staying) {
                println!("  {what}");
            }
        }
        Command::EmptyDeleted { yes, .. } => {
            // Everything it holds now, nothing deleted after this look: the counts only.
            let Some(list) = daemon::deleted(&paths, 0)? else {
                return Ok(not_running());
            };
            if list.accounts.is_empty() {
                println!("Recently deleted is empty.");
                return Ok(ExitCode::SUCCESS);
            }
            if list.total() == 0 {
                // Every folder in it still holds a live item: nothing goes for good yet.
                let shown = daemon::deleted(&paths, 20)?
                    .map(|l| l.items)
                    .unwrap_or_default();
                println!(
                    "Nothing in Recently deleted can be emptied yet: the folders in it still \
                     hold items that aren't deleted. In it:"
                );
                for what in described(&shown) {
                    println!("  {what}");
                }
                return Ok(ExitCode::SUCCESS);
            }
            let question = format!(
                "Delete the {} items in Recently deleted for good?",
                list.total()
            );
            if !yes && !ask_yes(&question)? {
                println!("Nothing deleted.");
                return Ok(ExitCode::SUCCESS);
            }
            daemon::empty_listed(&paths, &list.accounts)?;
            println!("{}", emptied(list.staying()));
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
        Command::Add {
            role,
            identity,
            folder,
        } => {
            let config = Config::load(&paths.config_file())?.unwrap_or_default();
            let identity = config.identity(identity.as_deref())?;
            // Refused (where setup refuses a folder, or another app syncs it) by the adding.
            let folder = match folder {
                Some(f) => setup::resolve(&f),
                None => setup::role_folder(role)?,
            };
            let what = format!(
                "{} syncs with {identity}'s {}",
                folder.display(),
                role.name()
            );
            match folders::add_synced(&paths, &identity, &folder, Some(role))? {
                Taken::Now => println!("{what}."),
                Taken::AtStart => println!("{what} once Filesync runs (`hbsync start`)."),
            }
        }
        Command::Remove { role, identity } => {
            let config = Config::load(&paths.config_file())?.unwrap_or_default();
            let identity = config.identity(identity.as_deref())?;
            let Some(root) = config.folder(&identity, Some(role)) else {
                println!("{identity}'s {} isn't synced here.", role.name());
                return Ok(ExitCode::SUCCESS);
            };
            folders::remove_synced(&paths, &root.folder)?;
            println!(
                "{} is no longer synced with {identity}. Its files stay.",
                root.folder.display()
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

fn parse_role(s: &str) -> Result<Role, String> {
    s.parse()
}

/// Items of Recently deleted as `hbsync deleted` shows them: where, when deleted, how many
/// items are in each, and which stay when emptied.
fn described(items: &[DeletedItem]) -> Vec<String> {
    let now = words::now();
    items
        .iter()
        .map(|item| {
            let along = match item.along {
                0 => String::new(),
                n => format!(" (with {n} items in it)"),
            };
            let stays = match item.deleted.stays {
                true => format!("; {}", words::STAYS),
                false => String::new(),
            };
            format!("{}{along}{stays}", words::deleted(item, now))
        })
        .collect()
}

/// What emptying does, `staying` items left (they hold a live node): never "deleting" alone
/// where something stays.
fn emptied(staying: usize) -> String {
    format!("Deleting for good. {}", words::staying(staying))
        .trim_end()
        .to_owned()
}

/// Asks `question` with [y/N]; whether the answer is yes.
fn ask_yes(question: &str) -> Res<bool> {
    print!("{question} [y/N] ");
    std::io::stdout().flush()?;
    let mut answer = String::new();
    std::io::stdin().lock().read_line(&mut answer)?;
    Ok(matches!(answer.trim(), "y" | "Y" | "yes"))
}

fn confirm_deletes(paths: &Paths, yes: bool) -> Res<()> {
    let Some(status) = daemon::status(paths)? else {
        return Err("the daemon isn't running (`hbsync start` starts it)".into());
    };
    let held: Vec<_> = status
        .folders
        .into_iter()
        .filter_map(|f| Some((f.path, f.held_deletes?)))
        .collect();
    if held.is_empty() {
        println!("No deletes are waiting for confirmation.");
        return Ok(());
    }
    for (path, held) in held {
        println!("{}:", path.display());
        for line in words::held_deletes(&held) {
            println!("{line}");
        }
        for example in &held.examples {
            println!("  {example}");
        }
        if held.more_than_shown() {
            println!("  ...");
        }
        if !yes && !ask_yes("Go ahead?")? {
            println!("Nothing deleted; they stay held.");
            continue;
        }
        // Exactly these: any delete since is held again, for another look.
        daemon::confirm_deletes(paths, &path, held.token)?;
        println!("Confirmed.");
    }
    Ok(())
}

fn print_status(status: &DaemonStatus) {
    for s in &status.folders {
        print_folder(s);
    }
    for a in &status.accounts {
        match &a.hints {
            Hints::Off | Hints::Connected => {}
            Hints::Connecting => println!("  notifications: connecting"),
            Hints::Lost(why) => println!(
                "  notifications: lost ({why}), reconnecting; meanwhile the drive is read every {} min",
                UNANNOUNCED_READ.as_secs() / 60
            ),
            Hints::Refused(why) => println!(
                "  notifications: refused ({why}), asked again later; meanwhile the drive is read every {} min",
                UNANNOUNCED_READ.as_secs() / 60
            ),
        }
        if let Some(w) = &a.credentials_warning {
            println!("  credentials: {w}");
        }
    }
}

fn print_folder(s: &FolderStatus) {
    println!("Homebase Filesync: {}", words::status_line(s, words::now()));
    let who = match s.role {
        Some(role) => format!("{}'s {}", s.identity, role.name()),
        None => s.identity.to_string(),
    };
    println!("  {who} <-> {}", s.path.display());
    // The status line has the progress, the pause, the next try and a failure's problem.
    if let Some(e) = s.error.as_ref().filter(|_| s.state != State::Failing) {
        println!("  problem: {e}");
    }
    if s.state == State::Denied {
        println!(
            "  authorize this computer again: hbsync setup {}",
            s.identity
        );
    }
    if let Some(at) = s.retry_at.filter(|_| s.state != State::Offline) {
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
    if s.pending > 0 && s.state != State::Idle {
        let items = if s.pending == 1 { "item" } else { "items" };
        println!("  {} {items} not synced yet", words::count(s.pending));
    }
    if let Some(h) = &s.held_deletes {
        println!(
            "  deletes held: {} here, {} elsewhere; `hbsync confirm-deletes` lets them through",
            h.here, h.remotely
        );
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
