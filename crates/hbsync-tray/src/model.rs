//! What the tray shows, worked out from what the daemon says: the icon's look, the tooltip and
//! the menu, as plain data (no GUI), so it can be tested.

use std::path::{Path, PathBuf};

use hbsync_app::runner::{State, Status};
use hbsync_app::words;
use hbsync_types::activity::{Synced, SyncedKind};

/// Entries of the recently synced list in the menu.
pub const RECENT_SHOWN: usize = 10;
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
    Running(Box<Status>),
}

impl Daemon {
    pub fn status(&self) -> Option<&Status> {
        match self {
            Daemon::Running(s) => Some(s),
            _ => None,
        }
    }
}

/// Everything the menu depends on.
#[derive(Debug, Clone)]
pub struct View<'a> {
    pub daemon: &'a Daemon,
    pub recent: &'a [Synced],
    pub start_at_login: bool,
    /// Something the tray itself is doing ("Logging in…").
    pub busy: Option<&'a str>,
    pub now: u64,
}

/// The icon: the mark, with a dot telling the state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Look {
    UpToDate,
    Busy,
    /// Paused, offline, not set up.
    Quiet,
    /// Something needs the user: a failure, held deletes, a refused login.
    Attention,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    LogIn,
    LogInAgain,
    OpenFolder,
    /// Show this file or folder in the file manager.
    Reveal(PathBuf),
    /// Pause for this many seconds.
    PauseFor(u64),
    Pause,
    Resume,
    SyncNow,
    ReviewDeletions,
    StartAtLogin,
    LogOut,
    OpenLogs,
    Quit,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    /// Text that can't be clicked.
    Label(String),
    Action(Action, String),
    Check(Action, String, bool),
    Submenu(String, Vec<Item>),
    Separator,
}

pub fn look(view: &View) -> Look {
    match view.daemon {
        Daemon::NotSetUp => Look::Quiet,
        Daemon::Starting(Some(_)) => Look::Attention,
        Daemon::Starting(None) => Look::Busy,
        Daemon::Running(s) => match s.state {
            State::Failing | State::Denied => Look::Attention,
            _ if s.held_deletes.is_some() => Look::Attention,
            State::Paused | State::Offline | State::Stopped => Look::Quiet,
            State::Starting | State::Syncing => Look::Busy,
            State::Idle if s.pending > 0 => Look::Busy,
            State::Idle => Look::UpToDate,
        },
    }
}

/// One line saying what's going on.
pub fn status_line(view: &View) -> String {
    if let Some(busy) = view.busy {
        return busy.to_owned();
    }
    match view.daemon {
        Daemon::NotSetUp => "Not connected".into(),
        Daemon::Starting(None) => "Starting…".into(),
        Daemon::Starting(Some(why)) => cut(&format!("Can't start: {why}")),
        Daemon::Running(s) => match s.state {
            State::Starting => "Catching up…".into(),
            State::Syncing => match &s.progress {
                Some(p) => format!("Syncing {}", words::progress(p)),
                None => "Syncing…".into(),
            },
            State::Idle if s.pending > 0 => format!("{} items waiting", s.pending),
            State::Idle => match &s.last_sync {
                Some(last) => format!(
                    "Up to date · synced {}",
                    words::when_roughly(last.at, view.now)
                ),
                None => "Up to date".into(),
            },
            State::Paused => match s.paused_until {
                Some(until) => format!("Paused · syncing again {}", words::when(until, view.now)),
                None => "Paused".into(),
            },
            State::Offline => match s.retry_at {
                Some(at) => format!("Offline · trying again {}", words::when(at, view.now)),
                None => "Offline".into(),
            },
            State::Failing => cut(&format!(
                "Problem: {}",
                s.error.as_deref().unwrap_or("syncing failed")
            )),
            State::Denied => "Homebase no longer accepts this computer".into(),
            State::Stopped => "Stopped".into(),
        },
    }
}

pub fn tooltip(view: &View) -> String {
    format!("Homebase Filesync · {}", status_line(view))
}

pub fn menu(view: &View) -> Vec<Item> {
    let mut items = vec![Item::Label(format!(
        "Homebase Filesync {}",
        env!("CARGO_PKG_VERSION")
    ))];
    let status = view.daemon.status();
    if let Some(s) = status {
        items.push(Item::Label(s.identity.clone()));
    }
    items.push(Item::Label(status_line(view)));
    items.push(Item::Separator);

    if *view.daemon == Daemon::NotSetUp {
        // Also while logging in: it opens the page that follows the login, or starts over.
        items.push(Item::Action(Action::LogIn, "Log in to Homebase…".into()));
        items.push(Item::Separator);
        items.push(Item::Action(Action::OpenLogs, "Open logs".into()));
        items.push(quit());
        return items;
    }

    items.push(Item::Action(
        Action::OpenFolder,
        "Open Homebase folder".into(),
    ));
    if let Some(s) = status {
        items.push(recently_synced(view.recent, &s.folder, view.now));
        let problems = problems(s, view.now);
        if !problems.is_empty() {
            items.push(Item::Submenu(
                format!("Problems ({})", s.recent.len()),
                problems,
            ));
        }
        if let Some(h) = &s.held_deletes {
            items.push(Item::Action(
                Action::ReviewDeletions,
                format!("Review {} deletions…", h.total()),
            ));
        }
        if s.state == State::Denied {
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
                Item::Label(cut(&format!("Your files stay in {}", s.folder.display()))),
                Item::Action(Action::LogOut, format!("Log out of {}", s.identity)),
            ],
        ));
    }
    items.push(Item::Separator);
    items.push(quit());
    items
}

fn quit() -> Item {
    Item::Action(Action::Quit, "Quit Homebase Filesync".into())
}

fn recently_synced(recent: &[Synced], folder: &Path, now: u64) -> Item {
    let mut entries: Vec<Item> = recent
        .iter()
        .take(RECENT_SHOWN)
        .map(|s| {
            let text = cut(&recent_line(s, now));
            match s.kind {
                // Gone from here: nothing to show.
                SyncedKind::DeletedRemotely | SyncedKind::TrashedLocally => Item::Label(text),
                _ => Item::Action(Action::Reveal(local_path(folder, &s.path)), text),
            }
        })
        .collect();
    if entries.is_empty() {
        entries.push(Item::Label("Nothing yet".into()));
    }
    Item::Submenu("Recently synced".into(), entries)
}

/// "report.txt · uploaded · 2 min ago", or "Photos/2024 · 312 files downloaded · 1 h ago".
pub fn recent_line(s: &Synced, now: u64) -> String {
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

fn problems(s: &Status, now: u64) -> Vec<Item> {
    s.recent
        .iter()
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
    use hbsync_app::runner::{HeldDeletes, LastSync, Note, Progress};

    const NOW: u64 = 1_800_000_000;

    fn status(state: State) -> Status {
        let mut s = Status::new("anna.example.com", Path::new("/home/anna/Homebase"));
        s.state = state;
        s
    }

    fn view<'a>(daemon: &'a Daemon, recent: &'a [Synced]) -> View<'a> {
        View {
            daemon,
            recent,
            start_at_login: true,
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
        let d = Daemon::Running(Box::new(s.clone()));
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
        let d = Daemon::Running(Box::new(s));
        let v = view(&d, &[]);
        assert_eq!(look(&v), Look::Busy);
        assert_eq!(status_line(&v), "Syncing 3 of 12 files, 1.0 MB of 40 MB");
    }

    #[test]
    fn paused_offers_resume() {
        let mut s = status(State::Paused);
        s.paused_until = Some(NOW + 3 * 3600);
        let d = Daemon::Running(Box::new(s));
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
        let d = Daemon::Running(Box::new(s));
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

        let d = Daemon::Running(Box::new(status(State::Denied)));
        let v = view(&d, &[]);
        assert_eq!(look(&v), Look::Attention);
        assert!(actions(&menu(&v)).contains(&Action::LogInAgain));
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
                kind: SyncedKind::TrashedLocally,
                path: "old.txt".into(),
                files: 1,
                bytes: 0,
            },
        ];
        let d = Daemon::Running(Box::new(status(State::Idle)));
        let v = view(&d, &recent);
        let m = menu(&v);
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
}
