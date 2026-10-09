//! The Homebase Filesync command line: sets syncing up and controls the daemon (`hbsyncd`).

use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::process::{Command as Process, ExitCode, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use hbsync_app::ipc::{self, Ask, IpcError, Reply, Request};
use hbsync_app::runner::{Hints, IDENTITY_KEY};
use hbsync_app::{
    Config, Control, CredentialStore, Location, Paths, Root, State, Status, autostart, setup,
};
use hbsync_local::{STATE_DIR, state_db_path};
use hbsync_state::StateDb;

type Res<T> = Result<T, Box<dyn std::error::Error>>;

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
        } => set_up(
            &paths,
            &identity,
            folder,
            existing,
            browser.as_deref(),
            !no_start,
            !no_autostart,
        )?,
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
            let Some(root) = configured(&paths)? else {
                println!("Not set up.");
                return Ok(ExitCode::SUCCESS);
            };
            stop(&paths)?;
            autostart_off_unless_dev()?;
            CredentialStore::open(&paths).delete(&root.identity)?;
            println!(
                "Logged out of {}. {} and its files stay; `hbsync setup {}` signs in again.",
                root.identity,
                root.folder.display(),
                root.identity
            );
        }
        Command::Unlink => {
            let path = paths.config_file();
            let Some(mut config) = Config::load(&path)? else {
                println!("Not set up.");
                return Ok(ExitCode::SUCCESS);
            };
            stop(&paths)?;
            autostart_off_unless_dev()?;
            for root in config.roots.drain(..) {
                println!(
                    "{} is no longer synced with {}. Its files stay.",
                    root.folder.display(),
                    root.identity
                );
            }
            config.save(&path)?;
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

fn configured(paths: &Paths) -> Res<Option<Root>> {
    Ok(Config::load(&paths.config_file())?.and_then(|c| c.root().ok().flatten().cloned()))
}

fn set_up(
    paths: &Paths,
    identity: &str,
    folder: Option<PathBuf>,
    existing: bool,
    browser: Option<&str>,
    start_now: bool,
    at_logon: bool,
) -> Res<()> {
    let identity = identity.trim().trim_end_matches('/').to_lowercase();
    let folder = match folder {
        Some(f) => std::path::absolute(f)?,
        None => home()?.join("Homebase"),
    };
    let config_path = paths.config_file();
    let mut config = Config::load(&config_path)?.unwrap_or_default();

    // The folder: new, empty, already this identity's, or adopted on purpose.
    std::fs::create_dir_all(&folder)?;
    let folder = folder.canonicalize()?;
    if let Some(root) = config.root()?
        && (root.identity != identity || root.folder != folder)
    {
        return Err(format!(
            "already set up to sync {} with {}: `hbsync unlink` first",
            root.folder.display(),
            root.identity
        )
        .into());
    }
    let state = folder.join(STATE_DIR);
    if state.is_dir() {
        let theirs = StateDb::open(&state_db_path(&folder))?.read(|s| s.get_kv(IDENTITY_KEY))?;
        if let Some(theirs) = theirs.filter(|t| *t != identity) {
            return Err(format!("{} is synced with {theirs}", folder.display()).into());
        }
    } else if !existing && std::fs::read_dir(&folder)?.next().is_some() {
        return Err(format!(
            "{} has files in it: they would be uploaded and merged with the drive. Add \
             --existing to do that, or pick an empty or new folder with --folder.",
            folder.display()
        )
        .into());
    }

    // This computer, authorized on the identity.
    let store = CredentialStore::open(paths);
    let mut creds = match store.load(&identity)? {
        Some((creds, _)) => {
            println!("This computer is already authorized on {identity}.");
            creds
        }
        None => {
            println!("Opening your browser to authorize this computer on {identity}...");
            let device = Config::load(&config_path)?
                .unwrap_or_default()
                .device_name();
            setup::authorize(&identity, &device, browser, |url| {
                println!("If it doesn't open, visit:\n{url}\n");
            })?
        }
    };
    setup::prepare(&mut creds)?;
    match store.save(&creds)? {
        Location::Keychain => println!("Credentials saved in the system keychain."),
        Location::File { path, why } => println!(
            "No keychain ({why}): credentials saved in {}, readable only by you.",
            path.display()
        ),
    }

    std::fs::create_dir_all(&state)?;
    StateDb::open(&state_db_path(&folder))?.write(|s| s.set_kv(IDENTITY_KEY, &identity))?;
    config.roots = vec![Root {
        identity: identity.clone(),
        folder: folder.clone(),
    }];
    config.save(&config_path)?;
    println!("{} syncs with {identity}.", folder.display());

    if at_logon {
        if dev_home() {
            println!("HBSYNC_HOME is set: not starting at logon (`hbsync autostart on` does).");
        } else {
            autostart::install(&autostart::daemon_exe()?)?;
            println!("The daemon starts at logon (`hbsync autostart off` stops that).");
        }
    }
    if !start_now {
        return Ok(());
    }
    if running(paths)? {
        stop(paths)?;
    }
    start(paths)?;
    println!("The daemon is running: `hbsync status` shows what it does.");
    Ok(())
}

/// A private instance (`HBSYNC_HOME`): tests, development. It leaves the user's logon entry
/// alone unless asked by `hbsync autostart`.
fn dev_home() -> bool {
    std::env::var_os("HBSYNC_HOME").is_some()
}

fn autostart_off_unless_dev() -> Res<()> {
    if !dev_home() && autostart::installed()? {
        autostart::uninstall()?;
        println!("The daemon no longer starts at logon.");
    }
    Ok(())
}

fn home() -> Res<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .ok_or_else(|| "no home folder (HOME unset): pass --folder".into())
}

fn ask(paths: &Paths, what: Ask) -> Result<Reply, IpcError> {
    ipc::call(paths, &Request::new(what))
}

fn control(paths: &Paths, c: Control) -> Res<()> {
    match ask(paths, Ask::Control(c)) {
        Ok(Reply::Done) => Ok(()),
        Ok(Reply::Refused(why)) => Err(why.into()),
        Ok(other) => Err(format!("unexpected answer: {other:?}").into()),
        Err(IpcError::NotRunning) => {
            Err("the daemon isn't running (`hbsync start` starts it)".into())
        }
        Err(e) => Err(e.into()),
    }
}

/// Whether a daemon answers.
fn running(paths: &Paths) -> Result<bool, IpcError> {
    match ask(paths, Ask::Status) {
        Ok(_) => Ok(true),
        Err(IpcError::NotRunning) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Starts the daemon and waits until it answers: through the service manager when it starts
/// the daemon at logon (so that it supervises it), else `hbsyncd` beside this program, on its
/// own.
fn start(paths: &Paths) -> Res<()> {
    if !dev_home() && autostart::installed()? && autostart::start()? {
        return wait_until_running(paths, None);
    }
    let exe = autostart::daemon_exe()?;
    let mut process = Process::new(&exe);
    process
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    std::os::unix::process::CommandExt::process_group(&mut process, 0);
    #[cfg(windows)]
    {
        // DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP: not tied to this console.
        std::os::windows::process::CommandExt::creation_flags(&mut process, 0x8 | 0x200);
    }
    let child = process
        .spawn()
        .map_err(|e| format!("can't start {}: {e}", exe.display()))?;
    wait_until_running(paths, Some(child))
}

/// Waits until the daemon answers, or `child` (when we started it) exits.
fn wait_until_running(paths: &Paths, mut child: Option<std::process::Child>) -> Res<()> {
    let until = Instant::now() + Duration::from_secs(15);
    while Instant::now() < until {
        if running(paths)? {
            return Ok(());
        }
        if let Some(status) = child.as_mut().map(|c| c.try_wait()).transpose()?.flatten() {
            return Err(format!(
                "the daemon exited ({status}); its log is in {}",
                paths.logs.display()
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(format!(
        "the daemon didn't answer; its log is in {}",
        paths.logs.display()
    )
    .into())
}

/// Stops the daemon and waits until it's gone. False when it wasn't running.
fn stop(paths: &Paths) -> Res<bool> {
    match ask(paths, Ask::Control(Control::Stop)) {
        Err(IpcError::NotRunning) => return Ok(false),
        Err(e) => return Err(e.into()),
        Ok(_) => {}
    }
    let until = Instant::now() + Duration::from_secs(60);
    while Instant::now() < until {
        if !running(paths)? {
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err("the daemon is still finishing a transfer; try again in a moment".into())
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
            "{} files deleted on this computer will be deleted from {} and your other devices.",
            held.here, status.identity
        );
    }
    if held.remotely > 0 {
        println!(
            "{} files deleted elsewhere will be moved to this computer's trash.",
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
    control(
        paths,
        Control::ConfirmDeletes {
            here: held.here,
            remotely: held.remotely,
        },
    )?;
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
