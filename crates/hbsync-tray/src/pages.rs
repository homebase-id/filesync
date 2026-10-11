//! The tray's pages, served on `127.0.0.1` and opened in the browser: logging in and choosing
//! the folder (the identity's approval happens in the browser anyway), and reviewing held
//! deletes. Every path starts with a random secret, so only the tray's own links reach them.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hbsync_app::control;
use hbsync_app::runner::Status;
use hbsync_app::setup::{self, FolderState};
use hbsync_app::{Config, Control, Paths};

use crate::{Event, Sender};

/// Requests and forms larger than this are refused.
const MAX_REQUEST: usize = 64 * 1024;

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
    /// Counts login attempts: an abandoned one (never approved) mustn't overwrite a later one.
    attempt: u64,
    /// The daemon's latest status, for the deletes page.
    status: Option<Status>,
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
                attempt: 0,
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

    /// Opens the login page, with `identity` and `folder` filled in (logging in again).
    pub fn prepare_setup(&self, identity: &str, folder: Option<PathBuf>) {
        let mut s = lock(&self.shared);
        if s.phase != Phase::Connecting {
            s.phase = Phase::Form;
            s.error = None;
        }
        if !identity.is_empty() {
            s.identity = identity.to_owned();
        }
        if folder.is_some() {
            s.folder = folder;
        }
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

/// A request, as far as the pages go.
#[derive(Debug, PartialEq)]
struct Request {
    method: String,
    path: String,
    form: HashMap<String, String>,
}

impl Server {
    fn serve(&self, mut stream: TcpStream) -> std::io::Result<()> {
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        let Some(request) = read_request(&mut stream)? else {
            return respond(&mut stream, 400, "text/plain", b"bad request");
        };
        let prefix = format!("/{}/", self.secret);
        let Some(page) = request.path.strip_prefix(&prefix) else {
            return respond(&mut stream, 404, "text/plain", b"not here");
        };
        let page = page.to_owned();
        match (request.method.as_str(), page.as_str()) {
            ("GET", "mark.png") => respond(&mut stream, 200, "image/png", crate::icons::MARK_256),
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
                self.pages.prepare_setup("", None);
                redirect(&mut stream, &self.pages.url("setup"))
            }
            ("GET", "deletions") => self.html(&mut stream, self.deletions_page(None)),
            ("POST", "deletions") => {
                let said = self.confirm_deletes(&request.form);
                self.html(&mut stream, self.deletions_page(Some(said)))
            }
            _ => respond(&mut stream, 404, "text/plain", b"not here"),
        }
    }

    fn html(&self, stream: &mut TcpStream, body: String) -> std::io::Result<()> {
        let page = frame(&self.pages.url("mark.png"), &body);
        respond(stream, 200, "text/html; charset=utf-8", page.as_bytes())
    }

    fn folder(&self) -> Option<PathBuf> {
        let chosen = lock(&self.pages.shared).folder.clone();
        chosen.or_else(|| setup::default_folder().ok())
    }

    /// Asks the tray (its main thread) for the OS's folder picker.
    fn choose_folder(&self) {
        let (tx, rx) = mpsc::channel();
        (self.send)(Event::PickFolder(tx));
        if let Ok(Some(folder)) = rx.recv() {
            let mut s = lock(&self.pages.shared);
            s.folder = Some(folder);
            s.merge_asked = false;
            s.error = None;
        }
    }

    fn setup_page(&self) -> String {
        let s = lock(&self.pages.shared);
        if s.phase != Phase::Form {
            drop(s);
            return self.done_page();
        }
        let folder = self.folder_shown(&s);
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

    fn folder_shown(&self, s: &Shared) -> String {
        s.folder
            .clone()
            .or_else(|| setup::default_folder().ok())
            .map_or_else(|| "(choose one)".into(), |f| f.display().to_string())
    }

    /// Checks the form and starts connecting. The browser goes to the identity's approval page
    /// (`Some`), or back to the form with what's wrong (`None`).
    fn start_setup(&self, form: &HashMap<String, String>) -> Option<String> {
        let identity = setup::normalize_identity(form.get("identity").map_or("", |s| s.as_str()));
        let merge = form.contains_key("merge");
        let fail = |why: String| {
            let mut s = lock(&self.pages.shared);
            s.identity.clone_from(&identity);
            s.error = Some(why);
            None
        };
        if !looks_like_a_domain(&identity) {
            return fail(format!(
                "{identity:?} doesn't look like a Homebase address (like anna.example.com)"
            ));
        }
        let Some(folder) = self.folder() else {
            return fail("choose a folder".into());
        };
        if let Some(why) = setup::refuse_folder(&folder, setup::home().as_deref()) {
            return fail(why);
        }
        let config = match Config::load(&self.paths.config_file()) {
            Ok(c) => c.unwrap_or_default(),
            Err(e) => return fail(e.to_string()),
        };
        match setup::check_folder(&config, &identity, &folder) {
            Ok(FolderState::HasFiles) if !merge => {
                let mut s = lock(&self.pages.shared);
                s.identity.clone_from(&identity);
                s.merge_asked = true;
                s.error = Some(
                    "This folder has files in it. Tick the box to merge them with your Homebase, \
                     or choose an empty or new folder."
                        .into(),
                );
                return None;
            }
            Ok(_) => {}
            Err(e) => return fail(e.to_string()),
        }

        let attempt = {
            let mut s = lock(&self.pages.shared);
            s.identity.clone_from(&identity);
            s.error = None;
            s.phase = Phase::Connecting;
            s.attempt += 1;
            s.attempt
        };
        (self.send)(Event::Wake);
        let (url_tx, url_rx) = mpsc::channel::<String>();
        let done = self.pages.url("setup/done");
        let request = setup::SetUp {
            identity: identity.clone(),
            folder: folder.clone(),
            existing: merge,
            start: true,
            at_logon: true,
        };
        let (paths, pages, send) = (self.paths.clone(), self.pages.clone(), self.send.clone());
        std::thread::spawn(move || {
            let mut authorize = |identity: &str, device: &str| {
                let pending = setup::begin(identity, device)?.return_to(&done);
                let _ = url_tx.send(pending.authorize_url.clone());
                pending.finish(setup::APPROVAL_TIMEOUT)
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
            let mut s = lock(&pages.shared);
            if s.attempt == attempt {
                s.phase = Phase::Done(result);
            }
            drop(s);
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
            Phase::Connecting => "<meta http-equiv=refresh content=1>\
                 <h1>Connecting…</h1><p>Approve this computer on your Homebase, in the page \
                 that opened. This page follows along.</p>"
                .to_owned(),
            Phase::Done(Ok(what)) => format!(
                "<h1>Connected</h1><p>{}</p><p>The Homebase icon in your tray shows what it \
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

    fn deletions_page(&self, said: Option<String>) -> String {
        if let Some(said) = said {
            return format!("<h1>Deletions</h1><p>{}</p>", escape(&said));
        }
        let s = lock(&self.pages.shared);
        let Some(status) = &s.status else {
            return "<h1>Deletions</h1><p>Filesync isn't running.</p>".into();
        };
        let Some(held) = &status.held_deletes else {
            return "<h1>Deletions</h1><p>No deletions are waiting.</p>".into();
        };
        let mut what = String::new();
        if held.here > 0 {
            what += &format!(
                "<p>{} items deleted on this computer will be deleted from your Homebase and \
                 your other devices.</p>",
                held.here
            );
        }
        if held.remotely > 0 {
            what += &format!(
                "<p>{} items deleted on another device will be moved to this computer's \
                 trash.</p>",
                held.remotely
            );
        }
        let examples: String = held
            .examples
            .iter()
            .map(|p| format!("<li><code>{}</code></li>", escape(p)))
            .collect();
        let more = if (held.examples.len() as u64) < held.here + held.remotely {
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
            held.here + held.remotely,
            url = self.pages.url("deletions"),
            token = escape(&held.token),
        )
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

/// `anna.example.com`, `frodo.dotyou.cloud:8443`: letters, digits, dots, hyphens, a port.
fn looks_like_a_domain(s: &str) -> bool {
    let host = s.split(':').next().unwrap_or_default();
    host.contains('.')
        && !host.starts_with('.')
        && !host.ends_with('.')
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':'))
}

fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<Request>> {
    let mut reader = BufReader::new(stream.take(MAX_REQUEST as u64));
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let mut parts = line.split_whitespace();
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else {
        return Ok(None);
    };
    let mut length = 0;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 {
            return Ok(None);
        }
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    if length > MAX_REQUEST {
        return Ok(None);
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    // A query (`?error=` from the identity's redirect) isn't needed: the pages know the outcome.
    let path = target.split_once('?').map_or(target, |(path, _)| path);
    Ok(Some(Request {
        method: method.to_owned(),
        path: path.to_owned(),
        form: parse_form(&String::from_utf8_lossy(&body)),
    }))
}

/// `a=1&b=x%20y` (also `+` for spaces).
fn parse_form(s: &str) -> HashMap<String, String> {
    s.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(k), decode(v))
        })
        .collect()
}

fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => match (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                (Some(high), Some(low)) => {
                    out.push(high << 4 | low);
                    i += 2;
                }
                _ => out.push(b'%'),
            },
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(b: u8) -> Option<u8> {
    (b as char).to_digit(16).map(|d| d as u8)
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn respond(stream: &mut TcpStream, status: u16, kind: &str, body: &[u8]) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status} {}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nConnection: close\r\n\r\n",
        if status == 200 { "OK" } else { "Error" },
        body.len()
    )?;
    stream.write_all(body)
}

fn redirect(stream: &mut TcpStream, url: &str) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 303 See Other\r\nLocation: {url}\r\nContent-Length: 0\r\n\
         Cache-Control: no-store\r\nConnection: close\r\n\r\n"
    )
}

/// A page in Homebase's look (design manual v2: the purple to blue gradient, Montserrat where
/// installed). Nothing is loaded from elsewhere.
fn frame(mark: &str, body: &str) -> String {
    format!(
        "<!doctype html><html lang=en><meta charset=utf-8>\
         <meta name=viewport content=\"width=device-width, initial-scale=1\">\
         <title>Homebase Filesync</title><link rel=icon href=\"{mark}\">\
         <style>{STYLE}</style>\
         <header><img src=\"{mark}\" alt=\"\"><span>Homebase Filesync</span></header>\
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
.error{color:#a0006e;background:#fde7f8;padding:10px 12px;border-radius:8px}\
.small{color:#666;font-size:14px}";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forms_decode() {
        let f = parse_form("identity=Anna.Example.com%2F&merge=on&x=a+b%");
        assert_eq!(f["identity"], "Anna.Example.com/");
        assert_eq!(f["merge"], "on");
        assert_eq!(f["x"], "a b%");
        assert_eq!(decode("%E2%9C%93"), "✓");
        assert_eq!(decode("%ä"), "%ä", "no panic in the middle of a character");
    }

    #[test]
    fn domains() {
        assert!(looks_like_a_domain("anna.example.com"));
        assert!(looks_like_a_domain("frodo.dotyou.cloud:8443"));
        assert!(!looks_like_a_domain("anna"));
        assert!(!looks_like_a_domain("anna.example.com/x"));
        assert!(!looks_like_a_domain(".example.com"));
        assert!(!looks_like_a_domain("a b.com"));
    }

    #[test]
    fn text_is_escaped() {
        assert_eq!(
            escape("<a href=\"x\">&"),
            "&lt;a href=&quot;x&quot;&gt;&amp;"
        );
    }

    #[test]
    fn only_the_secret_paths_answer() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let paths = Paths::under(dir.path());
        let send: Sender = Arc::new(|_| {});
        let pages = Pages::start(paths, send)?;
        let get = |url: &str| -> std::io::Result<String> {
            let rest = url.trim_start_matches("http://");
            let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
            let mut s = TcpStream::connect(host)?;
            write!(s, "GET /{path} HTTP/1.1\r\nHost: {host}\r\n\r\n")?;
            let mut out = String::new();
            s.read_to_string(&mut out)?;
            Ok(out)
        };
        let page = get(&pages.url("setup"))?;
        assert!(page.starts_with("HTTP/1.1 200"), "{page}");
        assert!(page.contains("Connect to your Homebase"));
        let base = pages.url("setup");
        let host = base
            .trim_start_matches("http://")
            .split('/')
            .next()
            .unwrap_or("");
        let guess = get(&format!("http://{host}/setup"))?;
        assert!(guess.starts_with("HTTP/1.1 404"), "{guess}");
        Ok(())
    }
}
