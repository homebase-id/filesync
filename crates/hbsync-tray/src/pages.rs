//! The tray's pages, served on `127.0.0.1` and opened in the browser: logging in and choosing
//! the folder (the identity's approval happens in the browser anyway), and reviewing held
//! deletes. Every path starts with a random secret, so only the tray's own links reach them.

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hbsync_app::control;
use hbsync_app::runner::Status;
use hbsync_app::setup::{self, Refusal};
use hbsync_app::words;
use hbsync_app::{Config, Control, Paths};
use hbsync_homebase::local_http::{escape, read_request, redirect, respond};

use crate::{Event, Sender};

#[derive(Debug, Clone, PartialEq)]
enum Phase {
    Form,
    /// Waiting for the identity's approval, then setting up.
    Connecting,
    Done(Result<String, String>),
}

#[derive(Debug)]
struct Shared {
    identity: String,
    folder: Option<PathBuf>,
    /// Asked to merge a folder with files in it.
    merge_asked: bool,
    error: Option<String>,
    phase: Phase,
    /// Gives up the login in progress (starting over): set, that login's outcome is ignored.
    cancel: Option<Arc<AtomicBool>>,
    /// The daemon's latest status, for the deletes page.
    status: Option<Status>,
}

impl Shared {
    /// The form, fresh.
    fn clear_form(&mut self) {
        self.phase = Phase::Form;
        self.error = None;
        self.merge_asked = false;
    }

    /// The folder chosen, or the default one.
    fn folder(&self) -> Option<PathBuf> {
        self.folder.clone().or_else(|| setup::default_folder().ok())
    }
}

/// The pages' server.
#[derive(Debug, Clone)]
pub struct Pages {
    base: String,
    shared: Arc<Mutex<Shared>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Pages {
    /// Starts serving on a free port.
    pub fn start(paths: Paths, send: Sender) -> std::io::Result<Pages> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let secret = uuid::Uuid::new_v4().simple().to_string();
        let base = format!(
            "http://127.0.0.1:{}/{secret}",
            listener.local_addr()?.port()
        );
        let pages = Pages {
            base,
            shared: Arc::new(Mutex::new(Shared {
                identity: String::new(),
                folder: None,
                merge_asked: false,
                error: None,
                phase: Phase::Form,
                cancel: None,
                status: None,
            })),
        };
        let server = Server {
            pages: pages.clone(),
            secret,
            paths,
            send,
        };
        std::thread::Builder::new()
            .name("hbsync-tray-pages".into())
            .spawn(move || {
                for stream in listener.incoming().flatten() {
                    let server = server.clone();
                    std::thread::spawn(move || {
                        if let Err(e) = server.serve(stream) {
                            tracing::debug!("a page request failed: {e}");
                        }
                    });
                }
            })?;
        Ok(pages)
    }

    /// The URL of `page` (`setup`, `deletions`).
    pub fn url(&self, page: &str) -> String {
        format!("{}/{page}", self.base)
    }

    /// Opens the login page, with `identity` and `folder` filled in (logging in again). A login
    /// waiting for approval goes on (its page offers to start over).
    pub fn prepare_setup(&self, identity: &str, folder: Option<PathBuf>) {
        let mut s = lock(&self.shared);
        if s.phase != Phase::Connecting {
            s.clear_form();
        }
        if !identity.is_empty() {
            s.identity = identity.to_owned();
        }
        if folder.is_some() {
            s.folder = folder;
        }
    }

    /// Gives up the login in progress, if any, and shows the form again.
    fn start_over(&self) {
        let mut s = lock(&self.shared);
        if let Some(cancel) = s.cancel.take() {
            cancel.store(true, Ordering::Relaxed);
        }
        s.clear_form();
    }

    /// Something the tray shows while a login goes on.
    pub fn busy(&self) -> Option<&'static str> {
        (lock(&self.shared).phase == Phase::Connecting)
            .then_some("Waiting for your approval in the browser…")
    }

    pub fn set_status(&self, status: Option<Status>) {
        lock(&self.shared).status = status;
    }
}

#[derive(Clone)]
struct Server {
    pages: Pages,
    secret: String,
    paths: Paths,
    send: Sender,
}

impl Server {
    fn serve(&self, mut stream: TcpStream) -> std::io::Result<()> {
        let Some(request) = read_request(&mut stream)? else {
            return respond(&mut stream, 400, "text/plain", b"bad request");
        };
        let prefix = format!("/{}/", self.secret);
        let Some(page) = request.path.strip_prefix(&prefix) else {
            return respond(&mut stream, 404, "text/plain", b"not here");
        };
        match (request.method.as_str(), page) {
            ("GET", "app-icon.png") => {
                respond(&mut stream, 200, "image/png", crate::icons::APP_256)
            }
            ("GET", "setup") => self.html(&mut stream, self.setup_page()),
            ("POST", "setup/folder") => {
                self.choose_folder();
                redirect(&mut stream, &self.pages.url("setup"))
            }
            ("POST", "setup") => match self.start_setup(&request.form) {
                Some(url) => redirect(&mut stream, &url),
                None => self.html(&mut stream, self.setup_page()),
            },
            ("GET", "setup/done") => self.html(&mut stream, self.done_page()),
            ("POST", "setup/again") => {
                self.pages.start_over();
                (self.send)(Event::Wake);
                redirect(&mut stream, &self.pages.url("setup"))
            }
            ("GET", "deletions") => self.html(&mut stream, self.deletions_page()),
            ("POST", "deletions") => {
                let said = self.confirm_deletes(&request.form);
                self.html(
                    &mut stream,
                    format!("<h1>Deletions</h1><p>{}</p>", escape(&said)),
                )
            }
            ("GET", "deleted") => self.html(&mut stream, self.deleted_page()),
            ("POST", "deleted/restore") => {
                let said = self.restore(&request.form);
                self.html(&mut stream, self.deleted_said(&said))
            }
            ("POST", "deleted/empty") => self.html(&mut stream, self.empty_page(&request.form)),
            ("POST", "deleted/empty/yes") => {
                let said = self.empty(&request.form);
                self.html(&mut stream, self.deleted_said(&said))
            }
            _ => respond(&mut stream, 404, "text/plain", b"not here"),
        }
    }

    fn html(&self, stream: &mut TcpStream, body: String) -> std::io::Result<()> {
        let page = frame(&self.pages.url("app-icon.png"), &body);
        respond(stream, 200, "text/html; charset=utf-8", page.as_bytes())
    }

    /// Asks the tray (its main thread) for the OS's folder picker.
    fn choose_folder(&self) {
        let (tx, rx) = mpsc::channel();
        (self.send)(Event::PickFolder(tx));
        if let Ok(Some(folder)) = rx.recv() {
            let mut s = lock(&self.pages.shared);
            s.folder = Some(folder);
            s.clear_form();
        }
    }

    fn setup_page(&self) -> String {
        let s = lock(&self.pages.shared);
        if s.phase != Phase::Form {
            drop(s);
            return self.done_page();
        }
        let folder = s
            .folder()
            .map_or_else(|| "(choose one)".into(), |f| f.display().to_string());
        let error = s
            .error
            .as_ref()
            .map(|e| format!("<p class=error>{}</p>", escape(e)))
            .unwrap_or_default();
        let merge = if s.merge_asked {
            "<label class=check><input type=checkbox name=merge> This folder already has files: \
             upload them and merge them with your Homebase</label>"
        } else {
            ""
        };
        format!(
            "<h1>Connect to your Homebase</h1>\
             <p>Filesync keeps a folder on this computer in sync with your Homebase, \
             encrypted.</p>{error}\
             <form method=post action=\"{setup}\">\
             <label for=identity>Your Homebase address</label>\
             <input id=identity name=identity placeholder=\"anna.example.com\" value=\"{identity}\" \
             autofocus required>\
             <label>Folder on this computer</label>\
             <div class=folder><code>{folder}</code>\
             <button class=secondary formaction=\"{choose}\" formnovalidate>Change…</button></div>\
             {merge}\
             <button>Continue</button>\
             </form>\
             <p class=small>Next, your Homebase asks you to approve this computer.</p>",
            setup = self.pages.url("setup"),
            choose = self.pages.url("setup/folder"),
            identity = escape(&s.identity),
            folder = escape(&folder),
        )
    }

    /// Checks the form and starts connecting, one login at a time. The browser goes to the
    /// identity's approval page (`Some`), or back to the form with what's wrong (`None`).
    fn start_setup(&self, form: &HashMap<String, String>) -> Option<String> {
        let typed = form.get("identity").map_or("", |s| s.as_str());
        let folder = lock(&self.pages.shared).folder();
        let planned = match (Config::load(&self.paths.config_file()), folder) {
            (Ok(config), Some(folder)) => setup::plan(
                &config.unwrap_or_default(),
                typed,
                &folder,
                form.contains_key("merge"),
                hbsync_app::paths::home().as_deref(),
            ),
            (Err(e), _) => Err(Refusal::No(e.to_string())),
            (_, None) => Err(Refusal::No("choose a folder".into())),
        };
        let request = match planned {
            Ok(request) => request,
            Err(refused) => {
                let mut s = lock(&self.pages.shared);
                s.identity = setup::normalize_identity(typed);
                s.error = Some(match refused {
                    Refusal::NeedsMerge(_) => {
                        s.merge_asked = true;
                        "This folder has files in it. Tick the box to merge them with your \
                         Homebase, or choose an empty or new folder."
                            .into()
                    }
                    Refusal::No(why) => why,
                });
                return None;
            }
        };

        let cancel = {
            let mut s = lock(&self.pages.shared);
            // A second submit (a double click) joins the login in progress.
            if s.phase == Phase::Connecting {
                return Some(self.pages.url("setup/done"));
            }
            s.clear_form();
            s.identity.clone_from(&request.identity);
            s.phase = Phase::Connecting;
            let cancel = Arc::new(AtomicBool::new(false));
            s.cancel = Some(Arc::clone(&cancel));
            cancel
        };
        (self.send)(Event::Wake);
        let (url_tx, url_rx) = mpsc::channel::<String>();
        let done = self.pages.url("setup/done");
        let (paths, pages, send) = (self.paths.clone(), self.pages.clone(), self.send.clone());
        std::thread::spawn(move || {
            let mut authorize = |identity: &str, device: &str| {
                let pending = setup::begin(identity, device)?.return_to(&done);
                let _ = url_tx.send(pending.authorize_url.clone());
                pending.finish_unless(setup::APPROVAL_TIMEOUT, &cancel)
            };
            let result = setup::set_up(&paths, &request, &mut authorize, &mut |line| {
                tracing::info!("{line}");
            });
            let result = result
                .map(|()| {
                    format!(
                        "{} syncs with {}.",
                        request.folder.display(),
                        request.identity
                    )
                })
                .map_err(|e| e.to_string());
            match &result {
                Ok(done) => tracing::info!("set up: {done}"),
                Err(e) => tracing::warn!("setting up failed: {e}"),
            }
            // Unless given up (started over): then a later login's page is showing.
            if !cancel.load(Ordering::Relaxed) {
                let mut s = lock(&pages.shared);
                s.phase = Phase::Done(result);
                s.cancel = None;
            }
            send(Event::Wake);
        });
        // The approval page, or (already authorized, or failed early) the outcome.
        Some(
            url_rx
                .recv_timeout(Duration::from_secs(60))
                .unwrap_or_else(|_| self.pages.url("setup/done")),
        )
    }

    fn done_page(&self) -> String {
        let s = lock(&self.pages.shared);
        match &s.phase {
            Phase::Form => "<h1>Not connected yet</h1>".to_owned(),
            Phase::Connecting => format!(
                "<meta http-equiv=refresh content=2>\
                 <h1>Connecting…</h1><p>Approve this computer on your Homebase, in the page \
                 that opened. This page follows along.</p>\
                 <form method=post action=\"{}\"><p class=small>A typo in the address, or the \
                 page closed? <button class=secondary>Start over</button></p></form>",
                self.pages.url("setup/again")
            ),
            Phase::Done(Ok(what)) => format!(
                "<h1>Connected</h1><p>{}</p><p>The Filesync icon in your tray shows what it \
                 does. You can close this page.</p>",
                escape(what)
            ),
            Phase::Done(Err(why)) => format!(
                "<h1>Not connected</h1><p class=error>{}</p>\
                 <form method=post action=\"{}\"><button>Try again</button></form>",
                escape(why),
                self.pages.url("setup/again")
            ),
        }
    }

    fn deletions_page(&self) -> String {
        let s = lock(&self.pages.shared);
        let Some(status) = &s.status else {
            return "<h1>Deletions</h1><p>Filesync isn't running.</p>".into();
        };
        let Some(held) = &status.held_deletes else {
            return "<h1>Deletions</h1><p>No deletions are waiting.</p>".into();
        };
        let what: String = words::held_deletes(held)
            .iter()
            .map(|line| format!("<p>{}</p>", escape(line)))
            .collect();
        let examples: String = held
            .examples
            .iter()
            .map(|p| format!("<li><code>{}</code></li>", escape(p)))
            .collect();
        let more = if held.more_than_shown() {
            "<li>…</li>"
        } else {
            ""
        };
        format!(
            "<h1>{} deletions are waiting</h1>\
             <p>Filesync holds back deletions this large until you confirm them.</p>{what}\
             <ul>{examples}{more}</ul>\
             <form method=post action=\"{url}\">\
             <input type=hidden name=token value=\"{token}\">\
             <button>Delete them</button></form>\
             <p class=small>Not sure? Leave this page: nothing is deleted, and the files stay \
             as they are on every device until you confirm.</p>",
            held.total(),
            url = self.pages.url("deletions"),
            token = escape(&held.token),
        )
    }

    /// Recently deleted, as the user deleted it: each item (a folder with what was in it),
    /// newest first.
    fn deleted_page(&self) -> String {
        let list = match control::deleted(&self.paths, usize::MAX) {
            Ok(Some(list)) => list,
            Ok(None) => return "<h1>Recently deleted</h1><p>Filesync isn't running.</p>".into(),
            Err(e) => {
                return format!(
                    "<h1>Recently deleted</h1><p class=error>{}</p>",
                    escape(&e.to_string())
                );
            }
        };
        if list.is_empty() {
            return "<h1>Recently deleted</h1><p>Nothing is in Recently deleted.</p>".into();
        }
        let now = words::now();
        // Shown: the items whose folder isn't in Recently deleted too; each with how many items
        // below it its restore brings back (those deleted with it).
        let by_node: HashMap<_, _> = list.iter().map(|d| (d.node, d)).collect();
        let mut along: HashMap<hbsync_types::NodeId, usize> = HashMap::new();
        for d in &list {
            let mut up = d.parent;
            for _ in 0..256 {
                let Some(folder) = by_node.get(&up) else {
                    break;
                };
                if !by_node.contains_key(&folder.parent) {
                    if folder.root == d.root {
                        *along.entry(folder.node).or_default() += 1;
                    }
                    break;
                }
                up = folder.parent;
            }
        }
        let rows: String = list
            .iter()
            .filter(|d| !by_node.contains_key(&d.parent))
            .take(MAX_DELETED)
            .map(|d| {
                let along = match along.get(&d.node).copied().unwrap_or(0) {
                    0 => String::new(),
                    n => format!(" <span class=small>with {n} items in it</span>"),
                };
                format!(
                    "<li><form method=post action=\"{url}\">\
                     <input type=hidden name=node value=\"{node}\">{what}{along} \
                     <button class=secondary>Restore</button></form></li>",
                    url = self.pages.url("deleted/restore"),
                    node = d.node.0,
                    what = escape(&words::deleted(d, now)),
                )
            })
            .collect();
        format!(
            "<h1>Recently deleted</h1>\
             <p>Deleted items are kept on your Homebase for {days} days, then deleted for good. \
             A restored item comes back on every device.</p><ul class=deleted>{rows}</ul>\
             <form method=post action=\"{empty}\"><input type=hidden name=count value={count}>\
             <input type=hidden name=until value={until}>\
             <button class=secondary>Empty now…</button></form>",
            days = self.keep_days(),
            empty = self.pages.url("deleted/empty"),
            count = list.len(),
            // Newest first: what the user sees, nothing deleted after this look.
            until = list[0].removed_at_ms,
        )
    }

    fn empty_page(&self, form: &HashMap<String, String>) -> String {
        let field = |name| escape(form.get(name).map_or("", String::as_str));
        format!(
            "<h1>Empty Recently deleted?</h1>\
             <p>The {count} items in it are deleted for good, from your Homebase and every \
             device. This can't be undone.</p>\
             <form method=post action=\"{url}\"><input type=hidden name=until value=\"{until}\">\
             <button>Delete them for good</button></form>\
             <p class=small>Changed your mind? Leave this page: nothing is deleted.</p>",
            count = field("count"),
            until = field("until"),
            url = self.pages.url("deleted/empty/yes"),
        )
    }

    /// Restores the item the form names.
    fn restore(&self, form: &HashMap<String, String>) -> String {
        let Some(node) = form
            .get("node")
            .and_then(|n| n.parse().ok())
            .map(hbsync_types::NodeId)
        else {
            return "Nothing was restored.".into();
        };
        self.ask_daemon(
            Control::Restore { nodes: vec![node] },
            "Restoring: it comes back with the next sync, on every device.",
        )
    }

    /// Empties Recently deleted of what the page showed (deleted until then).
    fn empty(&self, form: &HashMap<String, String>) -> String {
        let Some(at_ms) = form.get("until").and_then(|t| t.parse().ok()) else {
            return "Nothing was deleted.".into();
        };
        self.ask_daemon(
            Control::EmptyDeletedUntil { at_ms },
            "Emptying Recently deleted.",
        )
    }

    /// Sends `control` to the daemon; `done` if it took it, else why not.
    fn ask_daemon(&self, control: Control, done: &str) -> String {
        match control::control(&self.paths, control) {
            Ok(()) => {
                (self.send)(Event::Wake);
                done.into()
            }
            Err(e) => format!("That didn't work: {e}"),
        }
    }

    fn deleted_said(&self, said: &str) -> String {
        format!(
            "<h1>Recently deleted</h1><p>{}</p><p><a href=\"{}\">Back to Recently deleted</a></p>",
            escape(said),
            self.pages.url("deleted"),
        )
    }

    /// How long Recently deleted keeps items, from the config.
    fn keep_days(&self) -> u32 {
        Config::load(&self.paths.config_file())
            .ok()
            .flatten()
            .unwrap_or_default()
            .keep_deleted_days
            .0
    }

    fn confirm_deletes(&self, form: &HashMap<String, String>) -> String {
        let Some(token) = form.get("token") else {
            return "Nothing was confirmed.".into();
        };
        match control::control(
            &self.paths,
            Control::ConfirmDeletes {
                token: token.clone(),
            },
        ) {
            Ok(()) => {
                (self.send)(Event::Wake);
                "Confirmed: the deletions go through now.".into()
            }
            Err(e) => format!("Not confirmed: {e}"),
        }
    }
}

/// A page in Homebase's look (design manual v2: the purple to blue gradient, Montserrat where
/// installed). Nothing is loaded from elsewhere.
fn frame(icon: &str, body: &str) -> String {
    format!(
        "<!doctype html><html lang=en><meta charset=utf-8>\
         <meta name=viewport content=\"width=device-width, initial-scale=1\">\
         <title>Homebase Filesync</title><link rel=icon href=\"{icon}\">\
         <style>{STYLE}</style>\
         <header><img src=\"{icon}\" alt=\"\"><span>Homebase Filesync</span></header>\
         <main>{body}</main></html>"
    )
}

/// At most this many items of Recently deleted on its page.
const MAX_DELETED: usize = 1000;

const STYLE: &str = "\
:root{--purple:#603e90;--blue:#425985;--ink:#1d1d1b;--magenta:#eb00eb}\
*{box-sizing:border-box}\
body{margin:0;font:16px/1.5 Montserrat,'Open Sans',system-ui,sans-serif;color:var(--ink);\
background:#f4f2f8}\
header{display:flex;align-items:center;gap:12px;padding:20px 32px;color:#fff;font-weight:600;\
background:linear-gradient(120deg,var(--purple),var(--blue))}\
header img{width:36px;height:36px;border-radius:8px}\
main{max-width:560px;margin:32px auto;padding:32px;background:#fff;border-radius:16px;\
box-shadow:0 2px 12px rgba(0,0,0,.08)}\
h1{font-weight:600;font-size:24px;margin:0 0 12px}\
label{display:block;margin:20px 0 6px;font-weight:600}\
label.check{font-weight:400}\
input:not([type]),input[type=text]{width:100%;padding:10px 12px;font:inherit;\
border:1px solid #c9c3d6;border-radius:8px}\
.folder{display:flex;gap:12px;align-items:center}\
.folder code{flex:1;padding:10px 12px;background:#f4f2f8;border-radius:8px;overflow-wrap:anywhere}\
button{margin-top:24px;padding:10px 24px;font:inherit;font-weight:600;color:#fff;border:0;\
border-radius:999px;background:var(--purple);cursor:pointer}\
button.secondary{margin:0;color:var(--purple);background:#fff;border:1px solid var(--purple)}\
.error{color:#a0006e;background:#fde7f8;padding:10px 12px;border-radius:8px}\
.small{color:#666;font-size:14px}\
ul.deleted{padding:0;list-style:none}\
ul.deleted li{padding:8px 0;border-bottom:1px solid #eee;overflow-wrap:anywhere}\
ul.deleted button{margin-left:8px;padding:4px 14px}";

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::*;

    type R = Result<(), Box<dyn std::error::Error>>;

    /// Sends a request to the pages, returns the whole answer (failing, not hanging, if they
    /// don't answer).
    fn request(url: &str, method: &str, form: &str) -> std::io::Result<String> {
        let rest = url.trim_start_matches("http://");
        let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
        let mut s = TcpStream::connect(host)?;
        s.set_read_timeout(Some(Duration::from_secs(10)))?;
        write!(
            s,
            "{method} /{path} HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\n\r\n{form}",
            form.len()
        )?;
        let mut out = String::new();
        s.read_to_string(&mut out)?;
        Ok(out)
    }

    #[test]
    fn the_login_page_starts_nothing_until_the_form_is_right() -> R {
        let dir = tempfile::tempdir()?;
        let folder = dir.path().join("Mine");
        std::fs::create_dir(&folder)?;
        std::fs::write(folder.join("notes.txt"), "x")?;
        let send: Sender = Arc::new(|_| {});
        let pages = Pages::start(Paths::under(&dir.path().join("home")), send)?;
        pages.prepare_setup("", Some(folder.clone()));
        let nothing_started = || {
            let s = lock(&pages.shared);
            s.phase == Phase::Form && s.cancel.is_none()
        };

        let page = request(&pages.url("setup"), "POST", "identity=anna")?;
        assert!(page.contains("value=\"anna\""), "kept what was typed");
        assert_eq!(
            lock(&pages.shared)
                .error
                .as_deref()
                .map(|e| e.contains("look like")),
            Some(true)
        );
        assert!(nothing_started());

        request(&pages.url("setup"), "POST", "identity=anna.example.com")?;
        assert!(lock(&pages.shared).merge_asked);
        assert!(nothing_started());
        // A new login starts with a clean form.
        pages.prepare_setup("", None);
        assert!(!lock(&pages.shared).merge_asked);
        Ok(())
    }

    #[test]
    fn one_login_at_a_time_and_start_over() -> R {
        let dir = tempfile::tempdir()?;
        let send: Sender = Arc::new(|_| {});
        let pages = Pages::start(Paths::under(&dir.path().join("home")), send)?;
        pages.prepare_setup("", Some(dir.path().join("New")));
        // As if a login waits for approval: a second submit joins it, starting nothing.
        let cancel = Arc::new(AtomicBool::new(false));
        {
            let mut s = lock(&pages.shared);
            s.phase = Phase::Connecting;
            s.cancel = Some(Arc::clone(&cancel));
        }
        let answer = request(&pages.url("setup"), "POST", "identity=anna.example.com")?;
        assert!(
            answer.contains(&format!("Location: {}", pages.url("setup/done"))),
            "{answer}"
        );
        assert!(
            lock(&pages.shared)
                .cancel
                .as_ref()
                .is_some_and(|c| Arc::ptr_eq(c, &cancel))
        );
        assert!(pages.busy().is_some());
        // Starting over gives the waiting login up.
        request(&pages.url("setup/again"), "POST", "")?;
        assert!(cancel.load(Ordering::Relaxed));
        let s = lock(&pages.shared);
        assert_eq!((s.phase.clone(), s.cancel.is_none()), (Phase::Form, true));
        Ok(())
    }

    #[test]
    fn only_the_secret_paths_answer() -> R {
        let dir = tempfile::tempdir()?;
        let send: Sender = Arc::new(|_| {});
        let pages = Pages::start(Paths::under(dir.path()), send)?;
        let page = request(&pages.url("setup"), "GET", "")?;
        assert!(page.starts_with("HTTP/1.1 200"), "{page}");
        assert!(page.contains("Connect to your Homebase"));
        let base = pages.url("setup");
        let host = base
            .trim_start_matches("http://")
            .split('/')
            .next()
            .unwrap_or("");
        let guess = request(&format!("http://{host}/setup"), "GET", "")?;
        assert!(guess.starts_with("HTTP/1.1 404"), "{guess}");
        Ok(())
    }
}
