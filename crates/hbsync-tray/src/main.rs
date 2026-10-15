//! Homebase Filesync in the tray (the menu bar on macOS): what the daemon does, at a glance, and
//! what to do about it. It keeps the daemon running while a folder is set up, and quitting it
//! stops the daemon too. Logging in and reviewing held deletes happen in pages it serves to the
//! browser (`pages`).

// No console window.
#![cfg_attr(windows, windows_subsystem = "windows")]

mod icons;
mod model;
mod os;
mod pages;
mod poll;

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use hbsync_app::lock::{InstanceLock, LockError};
use hbsync_app::protocol::{DaemonControl, RecentItem};
use hbsync_app::{APP_NAME, Paths, autostart, control, shell, words};
use hbsync_app::{FolderStatus, State};
use tray_icon::menu::{
    CheckMenuItem, Menu, MenuEvent, MenuId, MenuItem, PredefinedMenuItem, Submenu,
};
use tray_icon::{Icon, TrayIcon, TrayIconBuilder};

use model::{Action, Daemon, Item, Look, View};
use pages::Pages;

/// What reaches the tray's main thread.
#[derive(Debug)]
pub enum Event {
    /// The watcher's latest look at the daemon, the recently synced list, and the program
    /// that replaced this one on disk ([`os::replacement`]).
    Daemon(Daemon, Vec<RecentItem>, Option<PathBuf>),
    Menu(MenuId),
    /// A page wants a folder chosen (the OS's picker runs on the main thread).
    PickFolder(mpsc::Sender<Option<PathBuf>>),
    /// Something changed: look again soon.
    Wake,
    /// Starting at login is now on (or off).
    StartAtLogin(bool),
    Exit,
}

/// Sends an [`Event`] to the main thread, from any thread.
pub type Sender = Arc<dyn Fn(Event) + Send + Sync>;

const PROGRAM: &str = autostart::TRAY;
/// A notification of one kind at most this often.
const NOTIFY_EVERY: Duration = Duration::from_secs(60);

fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("--version") {
        println!("{PROGRAM} {}", hbsync_app::VERSION);
        return ExitCode::SUCCESS;
    }
    let paths = match Paths::resolve() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{PROGRAM}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let _flush = match hbsync_app::logging::init(&paths, PROGRAM) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("{PROGRAM}: can't log to {}: {e}", paths.logs.display());
            return ExitCode::FAILURE;
        }
    };
    // Linux, started from a terminal (say): again, in a unit of the app's ([`hbsync_app::unit`]).
    #[cfg(target_os = "linux")]
    if let Ok(exe) = autostart::program_of(PROGRAM) {
        let args: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
        if hbsync_app::unit::relaunch(&exe, &args) {
            tracing::info!("started again in a unit of its own");
            return ExitCode::SUCCESS;
        }
    }
    let _lock = match InstanceLock::acquire_for(&paths, PROGRAM) {
        Ok(lock) => lock,
        // Started again (at logon, by hand): the one running is enough.
        Err(e @ LockError::Held { .. }) => {
            tracing::info!("{e}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            tracing::error!("{e}");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!("{PROGRAM} {} started", hbsync_app::VERSION);
    os::name_notifications();
    if let Some(why) = hbsync_app::install::Install::current().not_settled() {
        os::notify(APP_NAME.into(), format!("To start at login, {why}."));
    }
    // A private instance (HBSYNC_HOME) mustn't replace the launcher of the real one.
    #[cfg(target_os = "linux")]
    if !control::dev_home() {
        std::thread::spawn(|| {
            if let Err(e) = autostart::add_launcher(icons::APP_256) {
                tracing::info!("no launcher: {e}");
            }
        });
    }
    match run(paths) {
        Ok(None) => ExitCode::SUCCESS,
        // Linux, after an update: the new program in this process's place. The lock is
        // released (its file is closed on exec), and the new tray takes it.
        #[cfg(unix)]
        Ok(Some(exe)) => {
            tracing::info!("restarting into {}", exe.display());
            drop(_flush);
            let e = std::os::unix::process::CommandExt::exec(&mut std::process::Command::new(&exe));
            eprintln!("{PROGRAM}: can't restart into {}: {e}", exe.display());
            ExitCode::FAILURE
        }
        #[cfg(not(unix))]
        Ok(Some(_)) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("{e}");
            ExitCode::FAILURE
        }
    }
}

/// The tray's state, on the main thread.
struct App {
    paths: Paths,
    send: Sender,
    wake: mpsc::Sender<()>,
    pages: Pages,
    tray: Option<TrayIcon>,
    daemon: Daemon,
    recent: Vec<RecentItem>,
    start_at_login: bool,
    /// The program that replaced this one on disk ([`os::replacement`]), once seen.
    updated: Option<PathBuf>,
    /// Set by Restart: the program to run in this one's place once the loop ends.
    restart: Option<PathBuf>,
    shown: Shown,
    notified: HashMap<&'static str, Instant>,
    /// The folder given its icon and shortcut ([`shell`]) by this run.
    decorated: Option<PathBuf>,
}

/// What the tray shows now, to change only what changed.
#[derive(Default)]
struct Shown {
    look: Option<Look>,
    tooltip: String,
    items: Vec<Item>,
    /// The menu's entries, in the order [`flatten`] lists the items.
    handles: Vec<Handle>,
    actions: HashMap<MenuId, Action>,
}

enum Handle {
    Item(MenuItem),
    Check(CheckMenuItem),
    Sub(Submenu),
    Separator,
}

impl App {
    fn new(paths: Paths, send: Sender) -> Result<App, Box<dyn std::error::Error>> {
        let pages = Pages::start(paths.clone(), send.clone())?;
        let wake = poll::spawn(paths.clone(), send.clone())?;
        {
            let send = send.clone();
            MenuEvent::set_event_handler(Some(move |e: MenuEvent| send(Event::Menu(e.id))));
        }
        {
            let send = send.clone();
            std::thread::spawn(move || {
                send(Event::StartAtLogin(autostart::installed().unwrap_or(false)));
            });
        }
        Ok(App {
            paths,
            send,
            wake,
            pages,
            tray: None,
            daemon: Daemon::Starting(None),
            recent: Vec::new(),
            // Asked from a thread (the MSIX startup task takes a moment): Event::StartAtLogin.
            start_at_login: false,
            updated: None,
            restart: None,
            shown: Shown::default(),
            notified: HashMap::new(),
            decorated: None,
        })
    }

    /// Makes the tray icon: on the main thread, once its event loop runs.
    fn show(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let look = Look::Quiet;
        let (icon, template) = tray_icon_for(look)?;
        let builder = TrayIconBuilder::new()
            .with_tooltip(APP_NAME)
            .with_menu(Box::new(Menu::new()));
        #[cfg(target_os = "macos")]
        let builder = if template {
            builder.with_icon_templated(icon)
        } else {
            builder.with_icon(icon)
        };
        #[cfg(not(target_os = "macos"))]
        let builder = {
            debug_assert!(!template);
            builder.with_icon(icon)
        };
        self.tray = Some(builder.build()?);
        self.shown.look = Some(look);
        self.render();
        Ok(())
    }

    /// Handles one event; false to end.
    fn handle(&mut self, event: Event) -> bool {
        match event {
            Event::Daemon(daemon, recent, updated) => {
                self.notices(&daemon);
                if let Some(s) = daemon.status()
                    && self.decorated.as_ref() != Some(&s.path)
                    && s.path.is_dir()
                {
                    // Off this thread: some file managers take their time (PowerShell).
                    let (paths, folder, identity) =
                        (self.paths.clone(), s.path.clone(), s.identity.clone());
                    std::thread::spawn(move || {
                        shell::decorate(&paths, &folder, &identity, icons::FOLDER)
                    });
                    self.decorated = Some(s.path.clone());
                }
                self.pages.set_status(daemon.held_or_main().cloned());
                self.updated = updated;
                self.daemon = daemon;
                self.recent = recent;
            }
            Event::Menu(id) => {
                if let Some(action) = self.shown.actions.get(&id).cloned() {
                    return self.act(action);
                }
            }
            Event::PickFolder(reply) => {
                let picked = rfd::FileDialog::new()
                    .set_title("Choose the folder to sync")
                    .set_directory(hbsync_app::paths::home().unwrap_or_default())
                    .pick_folder();
                let _ = reply.send(picked);
            }
            Event::Wake => {
                let _ = self.wake.send(());
            }
            Event::StartAtLogin(on) => self.start_at_login = on,
            Event::Exit => return false,
        }
        self.render();
        true
    }

    fn status(&self) -> Option<&FolderStatus> {
        self.daemon.status()
    }

    fn act(&mut self, action: Action) -> bool {
        tracing::info!("menu: {action:?}");
        match action {
            Action::LogIn => {
                self.pages.prepare_setup("", None);
                os::open(self.pages.url("setup"));
            }
            Action::LogInAgain => {
                if let Some(s) = self.status() {
                    let (identity, folder) = (s.identity.clone(), s.path.clone());
                    self.pages.prepare_setup(&identity, Some(folder));
                }
                os::open(self.pages.url("setup"));
            }
            Action::OpenFolder => {
                let folder = match self.status() {
                    Some(s) => Some(s.path.clone()),
                    None => control::configured(&self.paths)
                        .ok()
                        .flatten()
                        .map(|r| r.folder),
                };
                if let Some(folder) = folder {
                    os::open(folder);
                }
            }
            Action::OpenOnline(url) => os::open(url),
            Action::Reveal(path) => {
                std::thread::spawn(move || os::reveal(&path));
            }
            Action::PauseFor(secs) => self.send_control(DaemonControl::Pause {
                until: Some(words::now() + secs),
            }),
            Action::Pause => self.send_control(DaemonControl::Pause { until: None }),
            Action::Resume => self.send_control(DaemonControl::Resume),
            Action::SyncNow => self.send_control(DaemonControl::SyncNow),
            Action::ReviewDeletions => os::open(self.pages.url("deletions")),
            Action::RecentlyDeleted => os::open(self.pages.url("deleted")),
            Action::Folders => os::open(self.pages.url("folders")),
            Action::StartAtLogin => {
                let (on, send) = (!self.start_at_login, self.send.clone());
                std::thread::spawn(move || {
                    let result = if on {
                        autostart::logon_program().and_then(|exe| autostart::install(&exe))
                    } else {
                        autostart::uninstall()
                    };
                    if let Err(e) = result {
                        os::notify("Start at login".into(), e.to_string());
                    }
                    send(Event::StartAtLogin(autostart::installed().unwrap_or(on)));
                });
            }
            Action::LogOut => {
                // Unlinking takes the folder's icon and shortcut off too.
                self.decorated = None;
                let (paths, send) = (self.paths.clone(), self.send.clone());
                std::thread::spawn(move || {
                    let done = control::log_out(&paths).and_then(|_| control::unlink(&paths));
                    if let Err(e) = done {
                        os::notify("Log out".into(), e.to_string());
                    }
                    send(Event::Wake);
                });
            }
            Action::OpenLogs => os::open(&self.paths.logs),
            Action::Restart => {
                // A login waiting for the browser comes back to this tray's pages.
                if let Some(busy) = self.pages.busy() {
                    os::notify(
                        "Restart".into(),
                        format!("After the login: {}", busy.to_lowercase()),
                    );
                    return true;
                }
                // The daemon goes on; the new tray restarts it if it's of another build.
                if let Some(exe) = self.updated.clone() {
                    self.tray = None;
                    self.restart = Some(exe);
                    return false;
                }
            }
            Action::Quit => {
                // Quitting stops syncing too, as people expect of a sync app. The icon goes at
                // once; the daemon may first finish a transfer.
                self.tray = None;
                let (paths, send) = (self.paths.clone(), self.send.clone());
                std::thread::spawn(move || {
                    if let Err(e) = control::stop(&paths) {
                        tracing::warn!("stopping the daemon: {e}");
                    }
                    tracing::info!("quit");
                    send(Event::Exit);
                });
                return true;
            }
        }
        self.render();
        true
    }

    /// Sends `c` to the daemon from a thread (it may take a moment), then looks again.
    fn send_control(&self, c: DaemonControl) {
        let (paths, send) = (self.paths.clone(), self.send.clone());
        std::thread::spawn(move || {
            if let Err(e) = control::daemon(&paths, c) {
                os::notify(APP_NAME.into(), e.to_string());
            }
            send(Event::Wake);
        });
    }

    fn notices(&mut self, next: &Daemon) {
        for (kind, summary, body) in notices(&self.daemon, next) {
            self.notify_once(kind, summary, body);
        }
    }

    fn notify_once(&mut self, kind: &'static str, summary: &str, body: String) {
        if self
            .notified
            .get(kind)
            .is_some_and(|at| at.elapsed() < NOTIFY_EVERY)
        {
            return;
        }
        self.notified.insert(kind, Instant::now());
        os::notify(summary.into(), body);
    }

    /// Brings the icon, tooltip and menu up to date, changing only what changed.
    fn render(&mut self) {
        let Some(tray) = &self.tray else { return };
        let busy = self.pages.busy();
        let view = View {
            daemon: &self.daemon,
            recent: &self.recent,
            start_at_login: self.start_at_login,
            updated: self.updated.is_some(),
            busy,
            now: words::now(),
        };
        let look = model::look(&view);
        if self.shown.look != Some(look) {
            let set = tray_icon_for(look).and_then(|(icon, template)| {
                #[cfg(target_os = "macos")]
                if template {
                    return Ok(tray.set_icon_templated(Some(icon))?);
                }
                debug_assert!(!template || cfg!(target_os = "macos"));
                Ok(tray.set_icon(Some(icon))?)
            });
            if let Err(e) = set {
                tracing::warn!("the icon: {e}");
            }
            self.shown.look = Some(look);
        }
        let tooltip = model::tooltip(&view);
        if self.shown.tooltip != tooltip {
            let _ = tray.set_tooltip(Some(&tooltip));
            self.shown.tooltip = tooltip;
        }
        let items = model::menu(&view);
        if items == self.shown.items {
            return;
        }
        if same_shape(&items, &self.shown.items) {
            // Only texts and ticks changed: those are set, and an open menu stays open.
            let was = flatten(&self.shown.items);
            for ((item, before), handle) in flatten(&items).iter().zip(was).zip(&self.shown.handles)
            {
                if *item == before {
                    continue;
                }
                match (item, handle) {
                    (Item::Label(t) | Item::Action(_, t), Handle::Item(h)) => h.set_text(t),
                    (Item::Check(_, t, on), Handle::Check(h)) => {
                        h.set_text(t);
                        h.set_checked(*on);
                    }
                    (Item::Submenu(t, _), Handle::Sub(h)) => h.set_text(t),
                    _ => {}
                }
            }
        } else {
            let menu = Menu::new();
            let mut handles = Vec::new();
            let mut actions = HashMap::new();
            for item in &items {
                let _ = menu.append(build(item, &mut handles, &mut actions).as_ref());
            }
            tray.set_menu(Some(Box::new(menu)));
            self.shown.handles = handles;
            self.shown.actions = actions;
        }
        self.shown.items = items;
    }
}

/// Notifications for what changed and needs the user, in any folder: a new conflict or
/// failure, held deletes, a refused login.
fn notices(before: &Daemon, next: &Daemon) -> Vec<(&'static str, &'static str, String)> {
    let (Daemon::Running(before), Daemon::Running(now)) = (before, next) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for now in &now.folders {
        if let Some(before) = before.folders.iter().find(|f| f.path == now.path) {
            out.extend(folder_notices(before, now));
        }
    }
    out
}

fn folder_notices(
    before: &FolderStatus,
    now: &FolderStatus,
) -> Vec<(&'static str, &'static str, String)> {
    let mut out = Vec::new();
    // The notes added since the last look (the newest are last).
    let added = now.notes_added.saturating_sub(before.notes_added) as usize;
    let fresh = &now.recent[now.recent.len().saturating_sub(added)..];
    if let Some(first) = fresh.first() {
        let body = match fresh.len() {
            1 => first.text.clone(),
            n => format!("{} (and {} more)", first.text, n - 1),
        };
        out.push(("problem", "Something needs a look", body));
    }
    let token = |s: &FolderStatus| s.held_deletes.as_ref().map(|h| h.token.clone());
    if let Some(held) = &now.held_deletes
        && token(before) != token(now)
    {
        out.push((
            "deletes",
            "Deletions waiting for you",
            format!(
                "{} deletions wait until you review them (in the menu).",
                held.total()
            ),
        ));
    }
    if now.state == State::Denied && before.state != State::Denied {
        out.push((
            "denied",
            "Log in again",
            "Your Homebase no longer accepts this computer: log in again from the menu.".into(),
        ));
    }
    out
}

/// The tray icon for `look`, and whether it is a template image (macOS).
fn tray_icon_for(look: Look) -> Result<(Icon, bool), Box<dyn std::error::Error>> {
    let (image, template) = icons::tray(look)?;
    Ok((
        Icon::from_rgba(image.pixels, image.width, image.height)?,
        template,
    ))
}

/// The menu entry for `item` (and its children), with its handles in [`flatten`]'s order.
fn build(
    item: &Item,
    handles: &mut Vec<Handle>,
    actions: &mut HashMap<MenuId, Action>,
) -> Box<dyn tray_icon::menu::IsMenuItem> {
    let mut id = |action: &Action| {
        let id = MenuId::new(actions.len().to_string());
        actions.insert(id.clone(), action.clone());
        id
    };
    match item {
        Item::Label(text) => {
            let h = MenuItem::new(text, false, None);
            handles.push(Handle::Item(h.clone()));
            Box::new(h)
        }
        Item::Action(action, text) => {
            let h = MenuItem::with_id(id(action), text, true, None);
            handles.push(Handle::Item(h.clone()));
            Box::new(h)
        }
        Item::Check(action, text, on) => {
            let h = CheckMenuItem::with_id(id(action), text, true, *on, None);
            handles.push(Handle::Check(h.clone()));
            Box::new(h)
        }
        Item::Submenu(text, children) => {
            let h = Submenu::new(text, true);
            handles.push(Handle::Sub(h.clone()));
            for child in children {
                let _ = h.append(build(child, handles, actions).as_ref());
            }
            Box::new(h)
        }
        Item::Separator => {
            handles.push(Handle::Separator);
            Box::new(PredefinedMenuItem::separator())
        }
    }
}

/// The items depth first, as [`build`] makes their handles.
fn flatten(items: &[Item]) -> Vec<&Item> {
    let mut out = Vec::new();
    for item in items {
        out.push(item);
        if let Item::Submenu(_, children) = item {
            out.extend(flatten(children));
        }
    }
    out
}

/// The same entries and actions, whatever their texts: the menu can be updated in place.
fn same_shape(a: &[Item], b: &[Item]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|pair| match pair {
            (Item::Label(_), Item::Label(_)) | (Item::Separator, Item::Separator) => true,
            (Item::Action(x, _), Item::Action(y, _)) | (Item::Check(x, ..), Item::Check(y, ..)) => {
                x == y
            }
            (Item::Submenu(_, x), Item::Submenu(_, y)) => same_shape(x, y),
            _ => false,
        })
}

/// Linux: the tray (StatusNotifierItem over D-Bus) runs on threads of its own; this thread
/// waits for events.
#[cfg(target_os = "linux")]
fn run(paths: Paths) -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
    let (tx, rx) = mpsc::channel();
    let tx = std::sync::Mutex::new(tx);
    let send: Sender = Arc::new(move |e| {
        if let Ok(tx) = tx.lock() {
            let _ = tx.send(e);
        }
    });
    let mut app = App::new(paths, send)?;
    app.show()?;
    for event in rx {
        if !app.handle(event) {
            break;
        }
    }
    Ok(app.restart.take())
}

/// Windows and macOS: the tray lives in the main thread's event loop.
#[cfg(not(target_os = "linux"))]
fn run(paths: Paths) -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
    use tao::event::{Event as TaoEvent, StartCause};
    use tao::event_loop::{ControlFlow, EventLoopBuilder};

    #[allow(unused_mut)]
    let mut event_loop = EventLoopBuilder::<Event>::with_user_event().build();
    // A menu bar app: no Dock icon, no app menu.
    #[cfg(target_os = "macos")]
    {
        use tao::platform::macos::{ActivationPolicy, EventLoopExtMacOS};
        event_loop.set_activation_policy(ActivationPolicy::Accessory);
    }
    let proxy = std::sync::Mutex::new(event_loop.create_proxy());
    let send: Sender = Arc::new(move |e| {
        if let Ok(p) = proxy.lock() {
            let _ = p.send_event(e);
        }
    });
    let mut app = App::new(paths, send)?;
    event_loop.run(move |event, _, flow| {
        *flow = ControlFlow::Wait;
        match event {
            TaoEvent::NewEvents(StartCause::Init) => {
                if let Err(e) = app.show() {
                    tracing::error!("can't show the tray icon: {e}");
                    *flow = ControlFlow::Exit;
                }
            }
            TaoEvent::UserEvent(e) => {
                if !app.handle(e) {
                    *flow = ControlFlow::Exit;
                }
            }
            _ => {}
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shapes() {
        let a = vec![
            Item::Label("Up to date".into()),
            Item::Submenu(
                "Recently synced".into(),
                vec![Item::Action(Action::OpenFolder, "x".into())],
            ),
        ];
        let mut b = a.clone();
        b[0] = Item::Label("Syncing 3 of 12 files".into());
        assert!(same_shape(&a, &b), "only a text changed");
        b[1] = Item::Submenu(
            "Recently synced".into(),
            vec![Item::Action(Action::SyncNow, "x".into())],
        );
        assert!(!same_shape(&a, &b), "an action changed");
        assert_eq!(flatten(&a).len(), 3);
    }
}
