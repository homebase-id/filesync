//! The Homebase Filesync command line: sets syncing up and controls the daemon (`hbsyncd`).

use std::io::{BufRead, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::mpsc::Receiver;

use clap::{Parser, Subcommand};
use hbsync_app::control::{running, start, stop_syncing};
use hbsync_app::daemon::UNANNOUNCED_READ;
use hbsync_app::folders::{self, Own, Taken};
use hbsync_app::protocol::{
    DaemonControl, DaemonStatus, DeletedItem, FolderStatus, Hints, Note, Pause, SharesFolder,
};
use hbsync_app::{
    APP_NAME, Config, HeldAnswer, Paths, Root, State, autostart, control as daemon, setup, words,
};
use hbsync_local::STATE_DIR;

type Res<T> = hbsync_app::Result<T>;

#[derive(Parser)]
#[command(
    name = "hbsync",
    version = hbsync_app::VERSION,
    about = format!("{APP_NAME}: keeps a folder in sync with your Homebase identity")
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
        /// The folder to sync [default: the main folder already set up with IDENTITY, else
        /// Homebase in your home folder].
        #[arg(long)]
        folder: Option<PathBuf>,
        /// Sync a folder that already has files in it (they are merged with the drive).
        #[arg(long)]
        existing: bool,
        #[command(flatten)]
        approve: Approve,
        /// Don't start the daemon.
        #[arg(long)]
        no_start: bool,
        /// Don't start the daemon at logon.
        #[arg(long)]
        no_autostart: bool,
    },
    /// Signs this computer in again (you approve in the browser): for a login from before
    /// private drives, which `hbsync status` names. The folders stay as they are.
    Login {
        /// The identity [default: the one set up].
        identity: Option<String>,
        #[command(flatten)]
        approve: Approve,
    },
    /// Lets Filesync read what others share with you: your Homebase asks you to approve it
    /// in the browser. Needed once, where Filesync was installed on your identity before it
    /// asked for this (`hbsync status` says it needs your approval); then what others share
    /// with you syncs.
    #[command(name = "approve")]
    ApproveSharing {
        /// The identity [default: the one set up].
        identity: Option<String>,
        #[command(flatten)]
        approve: Approve,
    },
    /// Syncs one of your own folders where this computer keeps it (documents, pictures,
    /// music, videos, downloads) with the drive's folder of that kind: joined if another device
    /// syncs it already (identical files are adopted, not copied), made if not. `desktop` syncs
    /// this computer's Desktop with a folder of its own in Desktops on the drive, which every
    /// other computer shows in its main folder.
    Add {
        what: Own,
        /// The identity to sync it with [default: the one set up].
        #[arg(long)]
        identity: Option<String>,
        /// Where this computer keeps it, if not where the OS says.
        #[arg(long)]
        folder: Option<PathBuf>,
        /// The Desktop's folder's name on the drive [default: the computer's]; given again for
        /// a Desktop synced already, it renames its folder.
        #[arg(long)]
        name: Option<String>,
    },
    /// Stops syncing one of your own folders. Its files stay; adding it again picks up where it
    /// was.
    Remove {
        what: Own,
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
    /// Lets through the deletions to review (the mass-delete guard's).
    ConfirmDeletes {
        /// Don't ask.
        #[arg(long)]
        yes: bool,
    },
    /// Puts the deletions to review back: what was deleted here comes back, what
    /// another device removed is restored on Homebase.
    PutBack {
        /// Don't ask.
        #[arg(long)]
        yes: bool,
    },
    /// Shows the folder what others share with you is synced into; with `--make` or
    /// `--take`, sets one up where it is gone.
    SharedFolder {
        /// Make a new one (the one there was is gone for good); with more than one identity
        /// whose folder is gone, in this Homebase folder.
        #[arg(long, conflicts_with = "take", value_name = "HOMEBASE_FOLDER", num_args = 0..=1)]
        make: Option<Option<PathBuf>>,
        /// Take this folder at the top of your Homebase folder as it: what was synced of it
        /// leaves your Homebase cloud and stays on this computer.
        #[arg(long, value_name = "FOLDER")]
        take: Option<PathBuf>,
        /// Don't ask (before a folder is taken).
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
            approve,
            no_start,
            no_autostart,
        } => {
            let config = Config::load(&paths.config_file())?.unwrap_or_default();
            let folder = match folder {
                Some(f) => f,
                None => setup::folder_for(&config, &identity)?,
            };
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
            let typed = approve.typed();
            let mut owner = approve.owner(typed.as_ref());
            setup::set_up(&paths, &request, &mut owner, &mut |line| println!("{line}"))?;
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
            println!("{}", words::restoring());
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
            println!("{}", words::emptying(staying.len(), "`hbsync status`"));
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
            println!("{}", words::emptying(list.staying(), "`hbsync status`"));
        }
        Command::ConfirmDeletes { yes } => answer_held(&paths, yes, HeldAnswer::Delete)?,
        Command::PutBack { yes } => answer_held(&paths, yes, HeldAnswer::PutBack)?,
        Command::SharedFolder { make, take, yes } => shared_folder(&paths, make, take, yes)?,
        Command::Start => {
            let was = running(&paths)?;
            // Also ends a stop that didn't go through.
            start(&paths)?;
            println!(
                "{}",
                if was {
                    "The daemon is already running."
                } else {
                    "Started."
                }
            );
        }
        Command::Stop => {
            if stop_syncing(&paths)? {
                println!("Stopped, until `hbsync start`.");
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
                println!("{APP_NAME} no longer starts at logon.");
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
        Command::Login { identity, approve } => {
            let config = Config::load(&paths.config_file())?.unwrap_or_default();
            let identity = config.identity(identity.as_deref())?;
            let typed = approve.typed();
            let mut owner = approve.owner(typed.as_ref());
            setup::sign_in_again(&paths, &identity, &mut owner)?;
            println!("Signed in again on {identity}.");
        }
        Command::ApproveSharing { identity, approve } => {
            let config = Config::load(&paths.config_file())?.unwrap_or_default();
            let identity = config.identity(identity.as_deref())?;
            let typed = approve.typed();
            let mut owner = approve.owner(typed.as_ref());
            match setup::approve(&paths, &identity, &mut owner)? {
                setup::Approved::Already => println!(
                    "{APP_NAME} may already read what others share with you on {identity}."
                ),
                setup::Approved::Given => {
                    println!(
                        "Approved on {identity}: {APP_NAME} may now read what others share \
                         with you."
                    )
                }
                setup::Approved::Declined => {
                    return Err("Not approved: what others share with you isn't synced.".into());
                }
            }
        }
        Command::Add {
            what,
            identity,
            folder,
            name,
        } => {
            let name = name
                .map(|n| folders::checked_desktop_name(&n))
                .transpose()?;
            if name.is_some() && what != Own::Desktop {
                return Err("--name is for the Desktop".into());
            }
            let config = Config::load(&paths.config_file())?.unwrap_or_default();
            let identity = config.identity(identity.as_deref())?;
            // The Desktop synced already is named anew where it is; another folder is refused
            // by the adding.
            let folder = match (folder, what, what.synced(&config, &identity)) {
                (Some(f), _, _) => setup::resolve(&f),
                (None, Own::Desktop, Some(synced)) => synced.folder.clone(),
                (None, Own::Role(role), _) => setup::role_folder(role)?,
                (None, Own::Desktop, None) => setup::desktop_folder()?,
            };
            let root = match what {
                Own::Role(role) => Root::role(&identity, &folder, role),
                Own::Desktop => folders::desktop(&config, &paths, &identity, &folder)?,
            };
            let what = format!("{} syncs with {}", folder.display(), root.what());
            let (taken, set_aside) = folders::add_synced(&paths, &root, name.as_deref())?;
            if let Some(why) = set_aside {
                println!("{}", words::set_aside(why));
            }
            match (taken, name) {
                (Taken::Now, Some(name)) => println!("{what}, named {name:?} on the drive."),
                (Taken::Now, None) => println!("{what}."),
                (Taken::AtStart, _) => {
                    println!("{what} once {APP_NAME} runs (`hbsync start`).")
                }
            }
        }
        Command::Remove { what, identity } => {
            let config = Config::load(&paths.config_file())?.unwrap_or_default();
            let identity = config.identity(identity.as_deref())?;
            let Some(root) = what.synced(&config, &identity) else {
                println!("{identity}'s {} isn't synced here.", what.name());
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
                println!("{APP_NAME} no longer starts at logon.");
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
            None if autostart::installed()? => println!("{APP_NAME} starts at logon."),
            None => println!("{APP_NAME} doesn't start at logon."),
            Some(OnOff::On) => {
                autostart::install(&autostart::logon_program()?)?;
                println!("{APP_NAME} starts at logon.");
                if let Some(hint) = autostart::hint() {
                    println!("{hint}.");
                }
            }
            Some(OnOff::Off) => {
                autostart::uninstall()?;
                println!("{APP_NAME} no longer starts at logon.");
            }
        },
        Command::Log => println!("{}", paths.logs.display()),
    }
    Ok(ExitCode::SUCCESS)
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

/// Asks `question` with [y/N]; whether the answer is yes.
fn ask_yes(question: &str) -> Res<bool> {
    print!("{question} [y/N] ");
    std::io::stdout().flush()?;
    let mut answer = String::new();
    std::io::stdin().lock().read_line(&mut answer)?;
    Ok(matches!(answer.trim(), "y" | "Y" | "yes"))
}

/// `hbsync shared-folder`: where what others share is synced into, and setting that folder
/// up again where it is gone.
fn shared_folder(
    paths: &Paths,
    make: Option<Option<PathBuf>>,
    take: Option<PathBuf>,
    yes: bool,
) -> Res<()> {
    let Some(status) = daemon::status(paths)? else {
        return Err(daemon::NotRunning.into());
    };
    let mains = status.folders.iter().filter(|f| f.is_main());
    let shared: Vec<(&FolderStatus, &SharesFolder)> = mains
        .filter_map(|f| Some((f, f.shares.as_ref()?)))
        .collect();
    if shared.is_empty() {
        // (A folder says what is shared into it once its sync has opened it: not while it
        // starts, is paused from the start, or can't open.)
        let none = "nothing is shared with you here, as far as the folders synced now tell";
        if make.is_some() || take.is_some() {
            return Err(none.into());
        }
        println!("Nothing is shared with you here (as far as the folders synced now tell).");
        return Ok(());
    }
    // Those whose folder isn't there: the ones to make or take one for.
    let there = |shares: &SharesFolder| matches!(shares, SharesFolder::At { .. });
    let off: Vec<&FolderStatus> = shared
        .iter()
        .filter(|(_, s)| !there(s))
        .map(|(f, _)| *f)
        .collect();
    // The Homebase folder meant: the one at `within`, or the only one whose shares are off.
    let meant = |within: Option<&Path>| -> Res<&FolderStatus> {
        match (within, off.as_slice()) {
            (Some(within), _) => {
                let within = setup::resolve(within);
                let found = shared.iter().map(|(f, _)| *f).find(|f| f.path == within);
                Ok(found.ok_or_else(|| format!("{} isn't a Homebase folder", within.display()))?)
            }
            (None, [one]) => Ok(*one),
            (None, []) => Err("the folder is there: nothing to do".into()),
            (None, _) => Err("say which Homebase folder: give its path".into()),
        }
    };
    // (Done by the daemon between syncs: one that can't be is told in `hbsync status`.)
    if let Some(within) = make {
        let folder = meant(within.as_deref())?;
        daemon::make_shares(paths, &folder.path)?;
        println!(
            "A new folder for what is shared with {} is being made in {}.",
            folder.identity,
            folder.path.display()
        );
        return Ok(());
    }
    if let Some(take) = take {
        // A path names its Homebase folder; a bare name is in the one whose shares are off.
        let name = take.file_name().and_then(|n| n.to_str());
        let name = name.ok_or("give the folder's name")?.to_owned();
        let folder = meant(take.parent().filter(|p| !p.as_os_str().is_empty()))?;
        // (Before the question: the daemon refuses the same. By the name the folder has.)
        let name = hbsync_local::shares::takeable(&folder.path, &name)?;
        let question = format!(
            "Take {name} in {} as the folder for what is shared with {}? What was synced of \
             it leaves your Homebase cloud (it stays on this computer, and in Recently deleted).",
            folder.path.display(),
            folder.identity
        );
        if !yes && !ask_yes(&question)? {
            println!("Nothing done.");
            return Ok(());
        }
        daemon::take_shares(paths, &folder.path, name.clone())?;
        println!(
            "{name} is being taken as the folder for what is shared with {}.",
            folder.identity
        );
        return Ok(());
    }
    for (folder, shares) in &shared {
        match (shares, words::shares_off_and_how(shares, SHARED_FOLDER)) {
            (SharesFolder::At { name }, _) => println!(
                "What is shared with {} is in {}",
                folder.identity,
                folder.path.join(name).display()
            ),
            (_, off) => println!("{}: {}", folder.identity, off.unwrap_or_default()),
        }
    }
    Ok(())
}

/// The command that shows and sets up the folder what others share is synced into.
const SHARED_FOLDER: &str = "hbsync shared-folder";

/// How the owner approves in the browser (`hbsync setup`, `hbsync login`).
#[derive(Debug, clap::Args)]
struct Approve {
    /// The browser to open the authorize page with.
    #[arg(long, value_name = "COMMAND")]
    browser: Option<String>,
    /// Approve in a browser on another computer: none is opened here, and the address that
    /// one ends on is pasted here [default: when this computer has no display, or is used
    /// over SSH, and you are at its terminal].
    #[arg(long, conflicts_with = "browser")]
    paste: bool,
}

/// Asked for a line typed since the last asking ([`setup::InBrowser::typed`]).
type Typed = Box<dyn Fn() -> Option<String>>;

impl Approve {
    /// Whether a browser is opened here, and with what: the one named; else the system's,
    /// unless the owner's browser is elsewhere (`--paste`, or none opened here would be one
    /// they see: [`setup::browser_here`]).
    fn opens(&self) -> setup::Opens<'_> {
        match &self.browser {
            Some(command) => setup::Opens::With(command),
            None if self.paste || !setup::browser_here() => setup::Opens::No,
            None => setup::Opens::System,
        }
    }

    /// The lines typed at the terminal, where the owner's answer is taken from a pasted
    /// address too: asked for (`--paste`, also from a pipe), or no browser is opened here
    /// and someone is at the terminal. Never from a terminal this process is in the
    /// background of ([`Approve::unread`] says so). Read by a thread of their own, from the
    /// first asking on (neither command reads anything else).
    fn typed(&self) -> Option<Typed> {
        let terminal = std::io::stdin().is_terminal();
        let wanted = self.paste || (self.opens() == setup::Opens::No && terminal);
        if !wanted || (terminal && !setup::terminal_in_foreground()) {
            return None;
        }
        let lines = std::cell::OnceCell::new();
        Some(Box::new(move || {
            let lines: &Receiver<String> = lines.get_or_init(typed_lines);
            lines.try_recv().ok()
        }))
    }

    /// What the link comes with where no address is read: that `--paste` has it read, or,
    /// asked for and not read (the command is in the terminal's background), why.
    fn unread(&self) -> &'static str {
        match (self.paste, &self.browser) {
            (true, _) => {
                "(--paste: the terminal can't be read while this runs in the background. Bring \
                 it to the foreground, or run it again there.)\n"
            }
            // (The two exclude each other.)
            (false, Some(_)) => {
                "(Approving in a browser on another computer? Run this again with --paste in \
                 place of --browser.)\n"
            }
            (false, None) => setup::PASTE_HINT,
        }
    }

    /// The owner's answers in the browser, as asked for, with `typed` ([`Approve::typed`]).
    fn owner<'a>(&'a self, typed: Option<&'a Typed>) -> setup::InBrowser<'a> {
        setup::InBrowser {
            opens: self.opens(),
            say: &|line| println!("{line}"),
            typed: typed.map(|typed| &**typed),
            unread: self.unread(),
        }
    }
}

/// The lines of standard input from now on, read by a thread of its own till the input ends
/// (or till nobody takes them any more). What was typed at a terminal before it began comes
/// at once, and isn't an answer to a link not shown yet: passed over ([`TYPED_AHEAD`]; a
/// pipe has nothing typed ahead, and a script may answer at once). A line that isn't text is
/// too; one longer than an address can be comes in pieces; and where more come than are
/// taken (a pipe), the reading waits.
fn typed_lines() -> Receiver<String> {
    let (tx, rx) = std::sync::mpsc::sync_channel(64);
    std::thread::spawn(move || {
        let began = std::time::Instant::now();
        let terminal = std::io::stdin().is_terminal();
        let mut input = std::io::stdin().lock();
        loop {
            let mut line = String::new();
            let read = (&mut input).take(LINE_AT_MOST).read_line(&mut line);
            let ahead = terminal && began.elapsed() < TYPED_AHEAD;
            let sent = match read {
                Ok(0) => false,
                Ok(_) => ahead || tx.send(line).is_ok(),
                Err(e) => e.kind() == std::io::ErrorKind::InvalidData,
            };
            if !sent {
                break;
            }
        }
    });
    rx
}

/// How long after the reading began a line counts as typed before it ([`typed_lines`]): what
/// waits in the terminal comes within a millisecond, and nobody pastes an address this fast.
const TYPED_AHEAD: std::time::Duration = std::time::Duration::from_millis(300);

/// A line read is at most this long (an address is about 450 bytes).
const LINE_AT_MOST: u64 = 8192;

fn answer_held(paths: &Paths, yes: bool, answer: HeldAnswer) -> Res<()> {
    let Some(status) = daemon::status(paths)? else {
        return Err(daemon::NotRunning.into());
    };
    let held: Vec<_> = status
        .folders
        .into_iter()
        .filter_map(|f| Some((f.path, f.sharer, f.state == State::Paused, f.held_deletes?)))
        .collect();
    if held.is_empty() {
        println!("No deletions to review.");
        return Ok(());
    }
    for (path, sharer, paused, held) in held {
        println!("{}:", path.display());
        let (delete, put_back) = ("`hbsync confirm-deletes`", "`hbsync put-back`");
        for line in words::held_deletes(&held, sharer.as_ref(), delete, put_back) {
            println!("{line}");
        }
        for example in &held.examples {
            println!("  {example}");
        }
        if held.more_than_shown() {
            println!("  ...");
        }
        // What another identity shares can't be put back: said, not asked.
        if answer == HeldAnswer::PutBack && sharer.is_some() {
            println!("Nothing done; they are still to review.");
            continue;
        }
        let question = match answer {
            HeldAnswer::Delete => "Delete them?",
            HeldAnswer::PutBack => "Put them back?",
        };
        if !yes && !ask_yes(question)? {
            println!("Nothing done; they are still to review.");
            continue;
        }
        // Exactly these: any delete since is held again, for another look.
        match answer {
            HeldAnswer::Delete => daemon::confirm_deletes(paths, &path, held.token)?,
            HeldAnswer::PutBack => daemon::put_back(paths, &path, held.token)?,
        }
        println!("{}", words::held_answered(answer, paused));
    }
    Ok(())
}

fn print_status(status: &DaemonStatus) {
    for s in &status.folders {
        print_folder(s);
    }
    for a in &status.accounts {
        let every = UNANNOUNCED_READ.as_secs() / 60;
        let hints = match &a.hints {
            Hints::Off | Hints::Connected => None,
            Hints::Connecting => Some("notifications: connecting".to_owned()),
            Hints::Lost(why) => Some(format!(
                "notifications: lost ({why}), reconnecting; meanwhile the drive is read every {every} min"
            )),
            Hints::Refused(why) => Some(format!(
                "notifications: refused ({why}), asked again later; meanwhile the drive is read every {every} min"
            )),
        };
        let credentials = a
            .credentials_warning
            .as_ref()
            .map(|w| format!("credentials: {w}"));
        // A login from before private drives: asked before the drive is made one.
        let login = a.login_without_key.then(|| {
            format!(
                "login: from before private drives: `hbsync login {}` signs in again",
                a.identity
            )
        });
        // What others share waits for the owner's approval of the app (a share synced
        // before says why itself; one that can't be opened yet has no folder to).
        let approval = a.needs_approval.then(|| {
            format!(
                "sharing: {APP_NAME} needs your approval to read what others share with you: \
                 `hbsync approve {}` asks your Homebase for it",
                a.identity
            )
        });
        // Recently deleted being deleted for good, in the background.
        let emptying = a.emptying.as_ref().map(words::emptying_status);
        if hints.is_none()
            && credentials.is_none()
            && login.is_none()
            && approval.is_none()
            && emptying.is_none()
            && a.recent.is_empty()
        {
            continue;
        }
        // Under its own heading: not read as the last folder's.
        println!("Account {}:", a.identity);
        for line in emptying
            .iter()
            .chain(&hints)
            .chain(&credentials)
            .chain(&login)
            .chain(&approval)
        {
            println!("  {line}");
        }
        // The account's own: Recently deleted's requests, files on the drive that don't read.
        print_notes("recently", &a.recent);
    }
}

fn print_folder(s: &FolderStatus) {
    println!("{APP_NAME}: {}", words::status_line(s, words::now()));
    let who = s.what();
    // What another identity shares comes one way (sync semantics §13).
    let arrow = if s.sharer.is_some() { "->" } else { "<->" };
    println!("  {who} {arrow} {}", s.path.display());
    // The status line has the progress, the pause, the next try and a failure's problem.
    // (As it does who a share waits for.)
    let said = matches!(s.state, State::Failing | State::Waiting);
    if let Some(e) = s.error.as_ref().filter(|_| !said) {
        println!("  problem: {e}");
    }
    // The status line says to log in again, and why where it is known: here is how (as the
    // tray's Log in again does it).
    if s.refused() {
        println!("  log in again: hbsync setup {}", s.identity);
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
            "  deletions to review: {} here, {} elsewhere; `hbsync confirm-deletes` lets them through, \
             `hbsync put-back` puts them back",
            h.here, h.remotely
        );
    }
    if let Some(off) = &s.shares
        && let Some(line) = words::shares_off_and_how(off, SHARED_FOLDER)
    {
        println!("  {line}");
    }
    if let Some(why) = &s.watcher_error {
        let every = s
            .rescan_secs
            .map_or_else(|| "from time to time".to_owned(), words::rescan_every);
        println!("  no live watcher ({why}): the folder is rescanned {every}");
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
    print_notes("recently", &s.recent);
}

/// The newest of `notes` (newest last) under `heading`, if any.
fn print_notes(heading: &str, notes: &[Note]) {
    if notes.is_empty() {
        return;
    }
    println!("  {heading}:");
    for note in notes.iter().rev().take(10) {
        println!("    {} {}", when(note.at), note.text);
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
