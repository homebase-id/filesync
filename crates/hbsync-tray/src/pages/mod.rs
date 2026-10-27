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

use hbsync_app::FolderStatus;
use hbsync_app::control;
use hbsync_app::local_http::{escape, read_request, redirect, respond};
use hbsync_app::protocol::AccountControl;
use hbsync_app::setup::{self, Refusal};
use hbsync_app::words;
use hbsync_app::{Config, Paths};

use crate::{Event, Sender};

mod deleted;
mod folders;
mod login;

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
    /// The OS's folder picker is open.
    picking: bool,
    /// Asked to merge a folder with files in it.
    merge_asked: bool,
    error: Option<String>,
    phase: Phase,
    /// Gives up the login in progress (starting over): set, that login's outcome is ignored.
    cancel: Option<Arc<AtomicBool>>,
    /// The daemon's latest status, for the deletes page.
    status: Option<FolderStatus>,
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
pub(crate) struct Pages {
    base: String,
    shared: Arc<Mutex<Shared>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Pages {
    /// Starts serving on a free port.
    pub(crate) fn start(paths: Paths, send: Sender) -> std::io::Result<Pages> {
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
                picking: false,
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
    pub(crate) fn url(&self, page: &str) -> String {
        format!("{}/{page}", self.base)
    }

    /// Opens the login page, with `identity` and `folder` filled in (logging in again). A login
    /// waiting for approval goes on (its page offers to start over).
    pub(crate) fn prepare_setup(&self, identity: &str, folder: Option<PathBuf>) {
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
    pub(crate) fn busy(&self) -> Option<&'static str> {
        (lock(&self.shared).phase == Phase::Connecting)
            .then_some("Waiting for your approval in the browser…")
    }

    pub(crate) fn set_status(&self, status: Option<FolderStatus>) {
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
                self.choose_folder(&request.form);
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
            ("GET", "folders") => self.html(&mut stream, self.folders_page(None)),
            ("POST", "folders/add") => {
                let said = self.change_folder(&request.form, true);
                self.html(&mut stream, self.folders_page(Some(&said)))
            }
            ("POST", "folders/remove") => {
                let said = self.change_folder(&request.form, false);
                self.html(&mut stream, self.folders_page(Some(&said)))
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
button.enter{position:absolute;left:-10000px}\
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
        // Enter in the address submits with the form's first button: Continue, not Change….
        let (_, form) = page.split_once("<form").ok_or("no form")?;
        let first = form.split("<button").nth(1).ok_or("no button")?;
        assert!(first.starts_with(" class=enter"), "{form}");
        assert!(first.contains(">Continue</button>"), "{form}");
        let choose = format!("formaction=\"{}\"", pages.url("setup/folder"));
        assert!(form.contains(&choose), "{form}");
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
    fn the_folders_page_lists_the_roles_once_logged_in() -> R {
        let dir = tempfile::tempdir()?;
        let send: Sender = Arc::new(|_| {});
        let paths = Paths::under(&dir.path().join("home"));
        let pages = Pages::start(paths.clone(), send)?;
        let page = request(&pages.url("folders"), "GET", "")?;
        assert!(page.contains("Log in to Homebase first"), "{page}");

        std::fs::create_dir_all(&paths.config)?;
        Config::update_for_test(&paths.config_file(), |c| {
            c.roots.push(hbsync_app::Root::main(
                &"anna.example.com".into(),
                &dir.path().join("Homebase"),
            ))
        })?;
        let page = request(&pages.url("folders"), "GET", "")?;
        for role in hbsync_types::Role::ALL {
            assert!(page.contains(role.name()), "{role:?}: {page}");
        }
        assert!(page.contains("Not recommended"), "Downloads");
        // Only roles it knows; nothing set up on this computer's real folders here.
        let page = request(&pages.url("folders/add"), "POST", "role=papers")?;
        assert!(page.contains("Nothing changed"), "{page}");
        Ok(())
    }

    /// Choosing the folder answers at once, the page saying where the picker is until it
    /// closes; one picker at a time, and no login while it's open; it keeps the address typed.
    /// From a stale page while a login runs, it does nothing.
    #[test]
    fn choosing_the_folder_keeps_the_address() -> R {
        let dir = tempfile::tempdir()?;
        // The pickers asked for (before the request is answered), answered by hand.
        let asked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let send: Sender = {
            let asked = Arc::clone(&asked);
            Arc::new(move |e| {
                if matches!(e, Event::PickFolder) {
                    asked.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        let asked = || asked.load(Ordering::Relaxed);
        let pages = Pages::start(Paths::under(&dir.path().join("home")), send)?;
        pages.prepare_setup("", None);
        let answer = request(
            &pages.url("setup/folder"),
            "POST",
            "identity=anna.example.com",
        )?;
        assert!(
            answer.contains(&format!("Location: {}", pages.url("setup"))),
            "{answer}"
        );
        assert_eq!(asked(), 1);
        let page = request(&pages.url("setup"), "GET", "")?;
        assert!(page.contains("the window that opened"), "{page}");
        assert!(page.contains("http-equiv=refresh"), "{page}");
        // A second click while it's open opens no second picker; Continue (from another tab)
        // starts nothing.
        request(&pages.url("setup/folder"), "POST", "identity=bob")?;
        assert_eq!(asked(), 1);
        let page = request(&pages.url("setup"), "POST", "identity=anna.example.com")?;
        assert!(page.contains("the window that opened"), "{page}");
        assert_eq!(lock(&pages.shared).phase, Phase::Form);

        let chosen = dir.path().join("Chosen");
        pages.picked(Some(chosen.clone()));
        let page = request(&pages.url("setup"), "GET", "")?;
        assert!(page.contains("value=\"anna.example.com\""), "{page}");
        assert!(
            page.contains(&*escape(&chosen.display().to_string())),
            "{page}"
        );
        assert!(!page.contains("the window that opened"), "{page}");

        // Closed without a choice: the folder stays.
        request(&pages.url("setup/folder"), "POST", "")?;
        assert_eq!(asked(), 2);
        pages.picked(None);
        assert_eq!(lock(&pages.shared).folder.as_ref(), Some(&chosen));

        // Should a login be under way when it closes: the login goes on, with its folder.
        request(&pages.url("setup/folder"), "POST", "")?;
        lock(&pages.shared).phase = Phase::Connecting;
        pages.picked(Some(dir.path().join("Late")));
        {
            let s = lock(&pages.shared);
            assert_eq!(
                (s.phase.clone(), s.folder.as_ref(), s.picking),
                (Phase::Connecting, Some(&chosen), false)
            );
        }
        request(
            &pages.url("setup/folder"),
            "POST",
            "identity=bob.example.com",
        )?;
        let s = lock(&pages.shared);
        assert_eq!(
            (s.phase.clone(), s.identity.as_str()),
            (Phase::Connecting, "anna.example.com")
        );
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
