//! What the tray shows, worked out from what the daemon says: the icon's look, the tooltip and
//! the menu, as plain data (no GUI), so it can be tested.

use std::path::{Path, PathBuf};

use hbsync_app::protocol::{AccountStatus, Note, RecentItem};
use hbsync_app::words;
use hbsync_app::{DaemonStatus, FolderStatus, State};
use hbsync_types::activity::{Synced, SyncedKind};

/// Entries of the recently synced list in the menu.
pub(crate) const RECENT_SHOWN: usize = 10;
/// Notes shown under Problems.
const PROBLEMS_SHOWN: usize = 8;
/// Menu text is cut to this many characters.
const WIDTH: usize = 60;

/// What the tray knows about the daemon.
#[derive(Debug, Clone, PartialEq)]
pub enum Daemon {
    /// Nothing set up: log in first.
    NotSetUp,
    /// Set up, not answering yet: being started, or failing to start (why).
    Starting(Option<String>),
    /// Set up, and stopped on purpose (quitting the tray and stopping syncing, `hbsync stop`):
    /// not started until the user starts it.
    Stopped,
    Running(Box<DaemonStatus>),
}

impl Daemon {
    /// The main folder's status, which the menu shows.
    pub fn status(&self) -> Option<&FolderStatus> {
        match self {
            Daemon::Running(s) => s.main_folder(),
            _ => None,
        }
    }

    /// The folder whose held deletes wait for the user (the main folder's first); else the main
    /// folder: what the deletions page shows.
    pub fn held_or_main(&self) -> Option<&FolderStatus> {
        let Daemon::Running(s) = self else {
            return None;
        };
        s.folders
            .iter()
            .filter(|f| f.held_deletes.is_some())
            .max_by_key(|f| f.role.is_none())
            .or_else(|| s.main_folder())
    }

    /// The account `identity` is signed in with a login from before private drives.
    pub fn login_without_key(&self, identity: &hbsync_app::Identity) -> bool {
        self.account(identity).is_some_and(|a| a.login_without_key)
    }

    /// The status of the account `identity`.
    pub fn account(&self, identity: &hbsync_app::Identity) -> Option<&AccountStatus> {
        let Daemon::Running(s) = self else {
            return None;
        };
        s.accounts.iter().find(|a| a.identity == *identity)
    }

    /// The status of the folder that most needs the user (the main folder among equals): what
    /// the status line and the icon say.
    pub fn most_urgent(&self) -> Option<&FolderStatus> {
        let Daemon::Running(s) = self else {
            return None;
        };
        s.folders
            .iter()
            .max_by_key(|f| (folder_look(f), f.role.is_none()))
    }
}

/// Everything the menu depends on.
#[derive(Debug, Clone)]
pub(crate) struct View<'a> {
    pub daemon: &'a Daemon,
    pub recent: &'a [RecentItem],
    pub start_at_login: bool,
    /// This program was replaced on disk (an update, on Linux): restarting runs the new one.
    pub updated: bool,
    /// Something the tray itself is doing ("Logging in…").
    pub busy: Option<&'a str>,
    pub now: u64,
}

/// The tray icon: the Filesync cloud, its dot telling the state.
/// Ordered by urgency: the icon shows the most urgent folder's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Look {
    UpToDate,
    /// Paused, offline, not set up.
    Quiet,
    Busy,
    /// Something needs the user: a failure, held deletes, a refused login.
    Attention,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Action {
    LogIn,
    LogInAgain,
    OpenFolder,
    /// Open this page in the browser.
    OpenOnline(String),
    /// Show this file or folder in the file manager.
    Reveal(PathBuf),
    /// Pause for this many seconds.
    PauseFor(u64),
    Pause,
    Resume,
    SyncNow,
    ReviewDeletions,
    RecentlyDeleted,
    Folders,
    StartAtLogin,
    LogOut,
    OpenLogs,
    /// Start the program that replaced this one, and end this one.
    Restart,
    /// Start the daemon stopped on purpose ([`Daemon::Stopped`]).
    StartSyncing,
    /// End the tray; the daemon goes on.
    Quit,
    /// End the tray and stop the daemon until it is started again.
    QuitAndStop,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Item {
    /// Text that can't be clicked.
    Label(String),
    Action(Action, String),
    Check(Action, String, bool),
    Submenu(String, Vec<Item>),
    Separator,
}

pub(crate) fn look(view: &View) -> Look {
    match view.daemon {
        Daemon::NotSetUp | Daemon::Stopped => Look::Quiet,
        Daemon::Starting(Some(_)) => Look::Attention,
        Daemon::Starting(None) => Look::Busy,
        Daemon::Running(_) => view.daemon.most_urgent().map_or(Look::Quiet, folder_look),
    }
}

fn folder_look(s: &FolderStatus) -> Look {
    match s.state {
        State::Failing | State::Denied => Look::Attention,
        _ if s.held_deletes.is_some() => Look::Attention,
        State::Paused | State::Offline | State::Stopped => Look::Quiet,
        State::Starting | State::Syncing => Look::Busy,
        State::Idle if s.pending > 0 => Look::Busy,
        State::Idle => Look::UpToDate,
    }
}

/// One line saying what's going on.
pub(crate) fn status_line(view: &View) -> String {
    if let Some(busy) = view.busy {
        return busy.to_owned();
    }
    match view.daemon {
        Daemon::NotSetUp => "Not connected".into(),
        Daemon::Starting(None) => "Starting…".into(),
        Daemon::Starting(Some(why)) => cut(&format!("Can't start: {why}")),
        Daemon::Stopped => "Syncing stopped".into(),
        Daemon::Running(_) => match view.daemon.most_urgent() {
            Some(s) => cut(&words::status_line(s, view.now)),
            None => "Nothing to sync".into(),
        },
    }
}

pub(crate) fn tooltip(view: &View) -> String {
    format!("Homebase Filesync · {}", status_line(view))
}

pub(crate) fn menu(view: &View) -> Vec<Item> {
    let mut items = vec![Item::Label(format!(
        "Homebase Filesync {}",
        hbsync_app::VERSION
    ))];
    if view.updated {
        items.push(Item::Action(
            Action::Restart,
            "Restart to finish the update".into(),
        ));
    }
    let status = view.daemon.status();
    if let Some(s) = status {
        items.push(Item::Label(s.identity.to_string()));
    }
    items.push(Item::Label(status_line(view)));
    // Recently deleted being deleted for good, in the background.
    let account = status.and_then(|s| view.daemon.account(&s.identity));
    if let Some(e) = account.and_then(|a| a.emptying.as_ref()) {
        items.push(Item::Label(cut(&words::emptying_status(e))));
    }
    items.push(Item::Separator);

    if *view.daemon == Daemon::NotSetUp {
        // Also while logging in: it opens the page that follows the login, or starts over.
        items.push(Item::Action(Action::LogIn, "Log in to Homebase…".into()));
        items.push(Item::Separator);
        items.push(Item::Action(Action::OpenLogs, "Open logs".into()));
        items.push(quit());
        return items;
    }

    if *view.daemon == Daemon::Stopped {
        items.push(Item::Action(Action::StartSyncing, "Start syncing".into()));
    }
    items.push(Item::Action(
        Action::OpenFolder,
        "Open Homebase folder".into(),
    ));
    if let Some(s) = status {
        items.push(Item::Action(
            Action::OpenOnline(hbsync_app::files_online(&s.identity)),
            "Browse files online".into(),
        ));
        items.push(recently_synced(view.recent, view.now));
        // The main folder's, and its account's own (Recently deleted's, the drive's).
        let account = view.daemon.account(&s.identity).map(|a| &a.recent[..]);
        let notes = [&s.recent[..], account.unwrap_or_default()];
        let problems = problems(&notes, view.now);
        if !problems.is_empty() {
            let count: usize = notes.iter().map(|n| n.len()).sum();
            items.push(Item::Submenu(format!("Problems ({count})"), problems));
        }
        if let Some(h) = view
            .daemon
            .held_or_main()
            .and_then(|f| f.held_deletes.as_ref())
        {
            items.push(Item::Action(
                Action::ReviewDeletions,
                format!("Review {} deletions…", h.total()),
            ));
        }
        items.push(Item::Action(
            Action::RecentlyDeleted,
            "Recently deleted…".into(),
        ));
        items.push(Item::Action(
            Action::Folders,
            "Set up this computer…".into(),
        ));
        // Refused, or a login from before private drives: asked before the drive is made one.
        if s.state == State::Denied || view.daemon.login_without_key(&s.identity) {
            items.push(Item::Action(Action::LogInAgain, "Log in again…".into()));
        }
        items.push(Item::Separator);
        if s.state == State::Paused {
            items.push(Item::Action(Action::Resume, "Resume syncing".into()));
        } else {
            items.push(Item::Submenu(
                "Pause syncing".into(),
                vec![
                    Item::Action(Action::PauseFor(3600), "For 1 hour".into()),
                    Item::Action(Action::PauseFor(8 * 3600), "For 8 hours".into()),
                    Item::Action(Action::PauseFor(24 * 3600), "For 24 hours".into()),
                    Item::Action(Action::Pause, "Until I resume".into()),
                ],
            ));
            items.push(Item::Action(Action::SyncNow, "Sync now".into()));
        }
    }
    items.push(Item::Separator);
    items.push(Item::Check(
        Action::StartAtLogin,
        "Start at login".into(),
        view.start_at_login,
    ));
    items.push(Item::Action(Action::OpenLogs, "Open logs".into()));
    if let Some(s) = status {
        items.push(Item::Submenu(
            "Log out".into(),
            vec![
                Item::Label(cut(&format!("Your files stay in {}", s.path.display()))),
                Item::Action(Action::LogOut, format!("Log out of {}", s.identity)),
            ],
        ));
    }
    items.push(Item::Separator);
    // Nothing runs to keep or stop while stopped, or failing to start.
    items.push(match view.daemon {
        Daemon::Stopped | Daemon::Starting(Some(_)) => quit(),
        _ => quit_or_stop(),
    });
    items
}

fn quit() -> Item {
    Item::Action(Action::Quit, "Quit Homebase Filesync".into())
}

/// Quitting while the daemon runs (or is being started) asks whether it goes on.
fn quit_or_stop() -> Item {
    Item::Submenu(
        "Quit Homebase Filesync".into(),
        vec![
            Item::Action(Action::Quit, "Keep syncing in the background".into()),
            Item::Action(Action::QuitAndStop, "Stop syncing too".into()),
        ],
    )
}

fn recently_synced(recent: &[RecentItem], now: u64) -> Item {
    let mut entries: Vec<Item> = recent
        .iter()
        .take(RECENT_SHOWN)
        .map(|item| {
            let s = &item.synced;
            let text = cut(&recent_line(s, now));
            match s.kind {
                // Gone from here: nothing to show.
                SyncedKind::DeletedRemotely | SyncedKind::DeletedLocally => Item::Label(text),
                _ => Item::Action(Action::Reveal(local_path(&item.folder, &s.path)), text),
            }
        })
        .collect();
    if entries.is_empty() {
        entries.push(Item::Label("Nothing yet".into()));
    }
    Item::Submenu("Recently synced".into(), entries)
}

/// "report.txt · uploaded · 2 min ago", or "Photos/2024 · 312 files downloaded · 1 h ago".
pub(crate) fn recent_line(s: &Synced, now: u64) -> String {
    let name = if s.is_group() {
        if s.path.is_empty() {
            "Homebase".to_owned()
        } else {
            s.path.clone()
        }
    } else {
        s.path.rsplit('/').next().unwrap_or(&s.path).to_owned()
    };
    format!(
        "{name} · {} · {}",
        words::what(s),
        words::when_roughly(s.at, now)
    )
}

/// The newest of `notes`' lists (each newest last), newest first.
fn problems(notes: &[&[Note]], now: u64) -> Vec<Item> {
    let mut all: Vec<&Note> = notes.iter().flat_map(|n| n.iter()).collect();
    // Stable: notes of one time keep their order.
    all.sort_by_key(|n| n.at);
    all.into_iter()
        .rev()
        .take(PROBLEMS_SHOWN)
        .map(|n| Item::Label(cut(&format!("{} · {}", n.text, words::when(n.at, now)))))
        .collect()
}

/// `rel` (`/`-separated, relative to the synced folder) on this computer.
fn local_path(folder: &Path, rel: &str) -> PathBuf {
    rel.split('/')
        .filter(|p| !p.is_empty())
        .fold(folder.to_path_buf(), |path, part| path.join(part))
}

/// At most [`WIDTH`] characters, cut in the middle (a path's ends tell the most).
fn cut(text: &str) -> String {
    let n = text.chars().count();
    if n <= WIDTH {
        return text.to_owned();
    }
    let keep = WIDTH - 1;
    let head: String = text.chars().take(keep / 2).collect();
    let tail: String = text.chars().skip(n - (keep - keep / 2)).collect();
    format!("{head}…{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use hbsync_app::protocol::{HeldDeletes, LastSync, Note, Progress};

    const NOW: u64 = 1_800_000_000;

    fn status(state: State) -> FolderStatus {
        let mut s = FolderStatus::new(&"anna.example.com".into(), Path::new("/home/anna/Homebase"));
        s.state = state;
        s
    }

    /// The daemon running with this one folder.
    fn running(folder: FolderStatus) -> Daemon {
        Daemon::Running(Box::new(DaemonStatus {
            version: hbsync_app::VERSION.into(),
            accounts: Vec::new(),
            folders: vec![folder],
        }))
    }

    fn view<'a>(daemon: &'a Daemon, recent: &'a [RecentItem]) -> View<'a> {
        View {
            daemon,
            recent,
            start_at_login: true,
            updated: false,
            busy: None,
            now: NOW,
        }
    }

    fn actions(items: &[Item]) -> Vec<Action> {
        let mut out = Vec::new();
        for item in items {
            match item {
                Item::Action(a, _) | Item::Check(a, _, _) => out.push(a.clone()),
                Item::Submenu(_, inner) => out.extend(actions(inner)),
                _ => {}
            }
        }
        out
    }

    #[test]
    fn a_role_folders_held_deletes_are_reviewed_from_the_menu() {
        let mut pictures =
            FolderStatus::new(&"anna.example.com".into(), Path::new("/home/anna/Pictures"));
        pictures.role = Some(hbsync_types::Role::Pictures);
        pictures.held_deletes = Some(HeldDeletes {
            here: 0,
            remotely: 300,
            examples: Vec::new(),
            token: "t".into(),
        });
        let daemon = Daemon::Running(Box::new(DaemonStatus {
            version: hbsync_app::VERSION.into(),
            accounts: Vec::new(),
            folders: vec![status(State::Idle), pictures],
        }));
        assert_eq!(
            daemon.held_or_main().map(|f| f.path.clone()),
            Some(PathBuf::from("/home/anna/Pictures"))
        );
        assert!(actions(&menu(&view(&daemon, &[]))).contains(&Action::ReviewDeletions));
    }

    /// Emptying Recently deleted shows in the menu while it runs, under the status line.
    #[test]
    fn emptying_shows_while_it_runs() {
        let Daemon::Running(mut daemon) = running(status(State::Idle)) else {
            panic!("running");
        };
        let mut account = hbsync_app::protocol::AccountStatus::new(&"anna.example.com".into());
        account.emptying = Some(hbsync_app::protocol::Emptying {
            done: 3000,
            total: 22_367,
            purge: false,
        });
        daemon.accounts.push(account);
        let d = Daemon::Running(daemon);
        let m = menu(&view(&d, &[]));
        let line = Item::Label("Emptying Recently deleted: 3,000 of 22,367".into());
        assert_eq!(m.iter().position(|i| *i == line), Some(3), "{m:?}");
        let idle = running(status(State::Idle));
        assert!(!menu(&view(&idle, &[])).iter().any(|i| matches!(
            i,
            Item::Label(l) if l.starts_with("Emptying")
        )));
    }

    #[test]
    fn not_set_up_offers_a_login() {
        let d = Daemon::NotSetUp;
        let v = view(&d, &[]);
        assert_eq!(look(&v), Look::Quiet);
        assert_eq!(status_line(&v), "Not connected");
        assert_eq!(
            actions(&menu(&v)),
            [Action::LogIn, Action::OpenLogs, Action::Quit]
        );
        // While logging in, the login stays at hand: its page can start over.
        let busy = View {
            busy: Some("Logging in…"),
            ..v
        };
        assert!(actions(&menu(&busy)).contains(&Action::LogIn));
        assert_eq!(status_line(&busy), "Logging in…");
    }

    /// Stopped on purpose: started from the menu, never by itself; quitting has nothing to
    /// stop.
    #[test]
    fn stopped_offers_to_start_syncing() {
        let d = Daemon::Stopped;
        let v = view(&d, &[]);
        assert_eq!(look(&v), Look::Quiet);
        assert_eq!(status_line(&v), "Syncing stopped");
        assert_eq!(
            actions(&menu(&v)),
            [
                Action::StartSyncing,
                Action::OpenFolder,
                Action::StartAtLogin,
                Action::OpenLogs,
                Action::Quit
            ]
        );
    }

    /// Quitting while the daemon runs, or is being started, asks whether it goes on.
    #[test]
    fn quitting_asks_whether_to_keep_syncing() {
        let quit = Item::Submenu(
            "Quit Homebase Filesync".into(),
            vec![
                Item::Action(Action::Quit, "Keep syncing in the background".into()),
                Item::Action(Action::QuitAndStop, "Stop syncing too".into()),
            ],
        );
        for d in [
            running(status(State::Idle)),
            running(status(State::Paused)),
            Daemon::Starting(None),
        ] {
            let m = menu(&view(&d, &[]));
            assert_eq!(m.last(), Some(&quit), "{d:?}");
            assert!(!actions(&m).contains(&Action::StartSyncing), "{d:?}");
        }
        // Nothing to keep running.
        for d in [
            Daemon::NotSetUp,
            Daemon::Starting(Some("the daemon exited".into())),
        ] {
            let m = menu(&view(&d, &[]));
            assert_eq!(m.last(), Some(&super::quit()), "{d:?}");
        }
    }

    #[test]
    fn up_to_date_and_syncing() {
        let mut s = status(State::Idle);
        s.last_sync = Some(LastSync {
            at: NOW - 120,
            millis: 5,
            done: 1,
            downloaded_changes: 0,
            adopted: 0,
        });
        let d = running(s.clone());
        let v = view(&d, &[]);
        assert_eq!(look(&v), Look::UpToDate);
        assert_eq!(status_line(&v), "Up to date · synced 2 min ago");
        let a = actions(&menu(&v));
        assert!(a.contains(&Action::PauseFor(3600)) && a.contains(&Action::SyncNow));
        assert!(a.contains(&Action::LogOut) && !a.contains(&Action::LogIn));

        s.state = State::Syncing;
        s.progress = Some(Progress {
            files_done: 3,
            files_total: 12,
            bytes_done: 1_000_000,
            bytes_total: 40_000_000,
        });
        let d = running(s);
        let v = view(&d, &[]);
        assert_eq!(look(&v), Look::Busy);
        assert_eq!(status_line(&v), "Syncing 3 of 12 files, 1.0 MB of 40 MB");

        // Start-up: catching up until its first sync has a plan, then that sync's progress.
        let mut s = status(State::Starting);
        let d = running(s.clone());
        let v = view(&d, &[]);
        assert_eq!(look(&v), Look::Busy);
        assert_eq!(status_line(&v), "Catching up…");
        s.progress = Some(Progress {
            files_done: 1_234,
            files_total: 19_000,
            bytes_done: 0,
            bytes_total: 0,
        });
        let d = running(s);
        let v = view(&d, &[]);
        assert_eq!(look(&v), Look::Busy);
        assert_eq!(status_line(&v), "Syncing 1,234 of 19,000 files");
    }

    #[test]
    fn paused_offers_resume() {
        let mut s = status(State::Paused);
        s.paused_until = Some(NOW + 3 * 3600);
        let d = running(s);
        let v = view(&d, &[]);
        assert_eq!(look(&v), Look::Quiet);
        assert_eq!(status_line(&v), "Paused · syncing again in 3 h");
        let a = actions(&menu(&v));
        assert!(a.contains(&Action::Resume));
        assert!(!a.contains(&Action::Pause) && !a.contains(&Action::SyncNow));
    }

    #[test]
    fn what_needs_the_user_shows() {
        let mut s = status(State::Idle);
        s.held_deletes = Some(HeldDeletes {
            here: 100,
            remotely: 20,
            examples: vec![],
            token: "t".into(),
        });
        s.recent = vec![Note {
            at: NOW - 60,
            text: "report.docx: edited on two devices".into(),
        }];
        let d = running(s);
        let v = view(&d, &[]);
        assert_eq!(look(&v), Look::Attention);
        let m = menu(&v);
        assert!(m.contains(&Item::Action(
            Action::ReviewDeletions,
            "Review 120 deletions…".into()
        )));
        assert!(m.iter().any(
            |i| matches!(i, Item::Submenu(t, inner) if t == "Problems (1)" && inner.len() == 1)
        ));
        // The account's own notes too, newest first.
        let Daemon::Running(mut both) = d else {
            panic!("running");
        };
        let mut account = hbsync_app::protocol::AccountStatus::new(&"anna.example.com".into());
        account.recent = vec![Note {
            at: NOW - 30,
            text: "Restoring from Recently deleted failed".into(),
        }];
        both.accounts.push(account);
        let d = Daemon::Running(both);
        let m = menu(&view(&d, &[]));
        let problems = m.iter().find_map(|i| match i {
            Item::Submenu(t, inner) if t == "Problems (2)" => Some(inner.clone()),
            _ => None,
        });
        let first = problems.and_then(|p| p.first().cloned());
        assert!(
            matches!(&first, Some(Item::Label(l)) if l.starts_with("Restoring")),
            "{first:?}"
        );

        let d = running(status(State::Denied));
        let v = view(&d, &[]);
        assert_eq!(look(&v), Look::Attention);
        assert!(actions(&menu(&v)).contains(&Action::LogInAgain));
    }

    /// A login from before private drives is asked to log in again while it still syncs.
    #[test]
    fn a_login_without_the_zero_knowledge_key_offers_logging_in_again() {
        let synced = running(status(State::Idle));
        assert!(!actions(&menu(&view(&synced, &[]))).contains(&Action::LogInAgain));
        let Daemon::Running(mut s) = synced else {
            unreachable!()
        };
        let mut account = AccountStatus::new(&"anna.example.com".into());
        account.login_without_key = true;
        s.accounts.push(account);
        let d = Daemon::Running(s);
        let v = view(&d, &[]);
        assert_eq!(look(&v), Look::UpToDate);
        assert!(actions(&menu(&v)).contains(&Action::LogInAgain));
    }

    /// The icon shows the most urgent folder's look.
    #[test]
    fn the_most_urgent_folder_sets_the_look() {
        let mut held = status(State::Idle);
        held.path = PathBuf::from("/home/anna/Documents");
        held.held_deletes = Some(HeldDeletes {
            here: 60,
            remotely: 0,
            examples: Vec::new(),
            token: "t".into(),
        });
        let d = Daemon::Running(Box::new(DaemonStatus {
            version: hbsync_app::VERSION.into(),
            accounts: Vec::new(),
            folders: vec![status(State::Idle), held],
        }));
        assert_eq!(look(&view(&d, &[])), Look::Attention);
        assert_eq!(
            d.most_urgent().map(|f| f.path.clone()),
            Some(PathBuf::from("/home/anna/Documents")),
            "the status line tells of the folder that needs the user"
        );
        assert_eq!(
            d.status().map(|s| s.path.clone()),
            Some(PathBuf::from("/home/anna/Homebase")),
            "the menu shows the main folder"
        );
    }

    #[test]
    fn recently_synced_entries() {
        let recent = vec![
            Synced {
                at: NOW - 30,
                kind: SyncedKind::Uploaded,
                path: "Docs/report.txt".into(),
                files: 1,
                bytes: 10,
            },
            Synced {
                at: NOW - 7200,
                kind: SyncedKind::Downloaded,
                path: "Photos/2024".into(),
                files: 312,
                bytes: 1_000_000,
            },
            Synced {
                at: NOW - 60,
                kind: SyncedKind::DeletedLocally,
                path: "old.txt".into(),
                files: 1,
                bytes: 0,
            },
        ];
        let recent: Vec<RecentItem> = recent
            .into_iter()
            .map(|synced| RecentItem {
                folder: PathBuf::from("/home/anna/Homebase"),
                main: true,
                synced,
            })
            .collect();
        let d = running(status(State::Idle));
        let v = view(&d, &recent);
        let m = menu(&v);
        assert!(
            m.contains(&Item::Action(
                Action::RecentlyDeleted,
                "Recently deleted…".into()
            )),
            "{m:?}"
        );
        assert!(
            m.contains(&Item::Action(
                Action::OpenOnline("https://anna.example.com/apps/chat/#filesync".into()),
                "Browse files online".into()
            )),
            "{m:?}"
        );
        let Some(Item::Submenu(_, entries)) = m
            .iter()
            .find(|i| matches!(i, Item::Submenu(t, _) if t == "Recently synced"))
        else {
            panic!("{m:?}")
        };
        assert_eq!(
            entries[0],
            Item::Action(
                Action::Reveal(PathBuf::from("/home/anna/Homebase/Docs/report.txt")),
                "report.txt · uploaded · just now".into()
            )
        );
        assert_eq!(
            entries[1],
            Item::Action(
                Action::Reveal(PathBuf::from("/home/anna/Homebase/Photos/2024")),
                "Photos/2024 · 312 files downloaded · 2 h ago".into()
            )
        );
        assert!(
            matches!(&entries[2], Item::Label(_)),
            "a delete can't be shown"
        );
    }

    #[test]
    fn long_text_is_cut_in_the_middle() {
        let long = format!("Problem: {}", "x".repeat(100));
        let c = cut(&long);
        assert_eq!(c.chars().count(), WIDTH);
        assert!(c.starts_with("Problem: ") && c.contains('…'));
    }

    #[test]
    fn an_update_asks_for_a_restart() {
        let daemon = Daemon::NotSetUp;
        let mut v = view(&daemon, &[]);
        assert!(!actions(&menu(&v)).contains(&Action::Restart));
        v.updated = true;
        assert_eq!(
            menu(&v)[1],
            Item::Action(Action::Restart, "Restart to finish the update".into())
        );
    }
}
