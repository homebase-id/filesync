//! Trips of the browser to the identity's owner app that come back to a page on localhost:
//! logging this device in (the YouAuth exchange, [`hbsync_homebase::youauth`]), and giving the
//! app its drive again.
//!
//! The browser may be on another computer (this one has none: set up over SSH). It can't
//! come back to this one's localhost then: the page it ends on doesn't load, and its address
//! holds the owner's answer. Pasted here ([`Paste`]), it is taken as the request would be.

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hbsync_homebase::app::Update;
use hbsync_homebase::{Credentials, HomebaseError, Pending};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

use crate::APP_NAME;
use crate::local_http;

const CALLBACK_PATH: &str = "/authorization-code-callback";
const DRIVE_PATH: &str = "/drive-callback";
const APPROVAL_PATH: &str = "/approval-callback";

/// Where on localhost the identity sends the browser back to: one path, waited for, and where
/// the browser goes on to from there.
#[derive(Debug)]
struct Callback {
    listener: TcpListener,
    path: String,
    /// Where the browser goes once it is back (`?error=` added on failure); else it shows a
    /// line of text.
    return_to: Option<String>,
}

/// What the user types while the browser is waited for: the address a browser on another
/// computer ended on ([`Login::finish_with`]).
#[derive(Clone, Copy)]
pub struct Paste<'a> {
    /// A line typed since the last asking, if there is one (asked ten times a second).
    pub typed: &'a dyn Fn() -> Option<String>,
    /// Told why a line wasn't taken.
    pub say: &'a dyn Fn(&str),
}

impl std::fmt::Debug for Paste<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Paste")
    }
}

/// How many typed lines are taken at one look (more wait for the next: a tenth of a second).
const LINES_A_LOOK: usize = 64;

/// The query the owner's answer came with.
type Query = HashMap<String, String>;

/// Said of a pasted line that isn't the address the browser was sent back to.
const NOT_THE_ADDRESS: &str = "That isn't the address the browser ended on after you approved: \
paste the whole address, starting with http://localhost";

impl Callback {
    /// The query of the address `line`, if that is the one the browser was sent back to:
    /// this callback's path, on any host and port. The whole address, or from its path on;
    /// `None` for anything else.
    fn pasted(&self, line: &str) -> Option<Query> {
        // (After the host, where there is one.)
        let from_path = match line.split_once("://") {
            Some((_, rest)) => &rest[rest.find('/')?..],
            None => line,
        };
        let (path, query) = local_http::target(from_path);
        (path == self.path).then_some(query)
    }

    /// Listens on a free port for `path`; the URL the browser is sent back to.
    fn bind(path: String) -> std::io::Result<(Callback, String)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let url = format!("http://localhost:{}{path}", listener.local_addr()?.port());
        let back = Callback {
            listener,
            path,
            return_to: None,
        };
        Ok((back, url))
    }

    /// Waits up to `timeout` for the owner's answer, given up as soon as `cancel` is set:
    /// its query, and the browser that brought it, waiting for an answer of its own; or the
    /// query of its address pasted (`paste`), where `ours` says it is this trip's answer.
    /// A line pasted that isn't is said, and waited on; an empty one is passed over.
    fn wait(
        &self,
        timeout: Duration,
        cancel: &AtomicBool,
        paste: Option<Paste<'_>>,
        ours: impl Fn(&Query) -> bool,
    ) -> Result<(Query, Option<TcpStream>), HomebaseError> {
        let deadline = Instant::now() + timeout;
        self.listener.set_nonblocking(true)?;
        // (What was typed before this trip began isn't its answer: an address pasted twice,
        // or after the browser came back by itself.)
        if let Some(paste) = paste {
            let before = std::iter::from_fn(paste.typed).take(LINES_A_LOOK).count();
            tracing::debug!("{before} lines typed before the browser was waited for: passed over");
        }
        loop {
            let (mut stream, _) = match self.listener.accept() {
                Ok(conn) => conn,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if cancel.load(Ordering::Relaxed) {
                        return Err(HomebaseError::Protocol("given up: started over".into()));
                    }
                    if Instant::now() > deadline {
                        return Err(HomebaseError::Protocol(
                            "no answer from the browser in time".into(),
                        ));
                    }
                    // Every line typed since the last look: the answer among them is taken;
                    // else, whatever was pasted, it is said once that it wasn't the address.
                    if let Some(paste) = paste {
                        let mut wrong = false;
                        for line in std::iter::from_fn(paste.typed).take(LINES_A_LOOK) {
                            match self.pasted(line.trim()).filter(&ours) {
                                Some(query) => return Ok((query, None)),
                                None => wrong |= !line.trim().is_empty(),
                            }
                        }
                        if wrong {
                            (paste.say)(NOT_THE_ADDRESS);
                        }
                    }
                    std::thread::sleep(Duration::from_millis(100));
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            stream.set_nonblocking(false)?;
            // Anything else is ignored: a favicon request, or a TLS handshake from a browser
            // that tries https first (Firefox's HTTPS-First) before falling back to http.
            let request = local_http::read_request(&mut stream).ok().flatten();
            match request.filter(|r| r.method == "GET" && r.path == self.path) {
                Some(request) => return Ok((request.query, Some(stream))),
                None => {
                    let _ = local_http::message(&mut stream, 404, "Not here.");
                }
            }
        }
    }

    /// Answers the browser that came back (none: its address was pasted): sent on to
    /// `return_to` (with `?error=<why>` on failure), else a line of text. A browser gone
    /// meanwhile (the tab closed) changes nothing: the owner's answer came.
    fn answer(&self, stream: Option<TcpStream>, outcome: Result<&str, String>) {
        let Some(mut stream) = stream else {
            return;
        };
        let stream = &mut stream;
        let answered = match (&self.return_to, outcome) {
            (Some(url), Ok(_)) => local_http::redirect(stream, url),
            (Some(url), Err(why)) => {
                let why = utf8_percent_encode(&why, NON_ALPHANUMERIC).to_string();
                let sep = if url.contains('?') { '&' } else { '?' };
                local_http::redirect(stream, &format!("{url}{sep}error={why}"))
            }
            (None, outcome) => {
                let text = match outcome {
                    Ok(done) => format!("{APP_NAME} {done}. You can close this window."),
                    Err(why) => why,
                };
                local_http::message(stream, 200, &text)
            }
        };
        if let Err(e) = answered {
            tracing::debug!("the browser didn't take the answer: {e}");
        }
    }
}

/// A login waiting for the browser to come back.
#[derive(Debug)]
pub struct Login {
    pending: Pending,
    back: Callback,
}

/// Starts logging in to `identity` (a domain such as `frodo.dotyou.cloud`); `friendly_name`
/// names the device in the owner's list.
pub(crate) fn begin(identity: &str, friendly_name: &str) -> Result<Login, HomebaseError> {
    // The identity's answer carries the request's state.
    let (back, redirect_uri) = Callback::bind(CALLBACK_PATH.to_owned())?;
    Ok(Login {
        pending: hbsync_homebase::youauth::begin(identity, friendly_name, &redirect_uri)?,
        back,
    })
}

impl Login {
    /// Open this in the browser.
    pub fn authorize_url(&self) -> &str {
        &self.pending.authorize_url
    }

    /// Sends the browser to `url` once the identity is back, instead of showing a line of text
    /// (`?error=<why>` added when connecting failed).
    pub fn return_to(mut self, url: &str) -> Self {
        self.back.return_to = Some(url.to_owned());
        self
    }

    /// Waits up to `timeout` for the browser's callback, then exchanges it for credentials;
    /// given up as soon as `cancel` is set (the user started over).
    pub fn finish_unless(
        self,
        timeout: Duration,
        cancel: &AtomicBool,
    ) -> Result<Credentials, HomebaseError> {
        self.finish_with(timeout, cancel, None)
    }

    /// [`Login::finish_unless`], the owner's answer taken also from the address the browser
    /// ended on, pasted (`paste`): for a browser on another computer, which can't come back
    /// to this one. An address of another login, or one with a part lost, is said, and
    /// waited on ([`Pending::answers`]).
    pub(crate) fn finish_with(
        self,
        timeout: Duration,
        cancel: &AtomicBool,
        paste: Option<Paste<'_>>,
    ) -> Result<Credentials, HomebaseError> {
        let ours = |query: &Query| self.pending.answers(query);
        let (query, browser) = self.back.wait(timeout, cancel, paste, ours)?;
        let result = self.pending.complete(&query);
        let outcome = match &result {
            Ok(_) => Ok("is connected"),
            Err(e) => Err(format!("{APP_NAME} could not connect: {e}")),
        };
        self.back.answer(browser, outcome);
        result
    }
}

/// The owner giving the installed app more on their Homebase's app-update page
/// ([`Update`]: its drive again, or the permission to read what others share with them),
/// waiting for the browser to come back.
#[derive(Debug)]
pub struct Asked {
    url: String,
    back: Callback,
    what: Update,
}

/// Starts asking the owner of `identity` to give the app its drive again.
pub(crate) fn begin_restore(identity: &str) -> Result<Asked, HomebaseError> {
    begin_update(identity, Update::Drive)
}

/// Starts asking the owner of `identity` to let the app read what others share with them
/// ([`Update::Permission`]: an install from before it asked for that).
pub(crate) fn begin_approval(identity: &str) -> Result<Asked, HomebaseError> {
    begin_update(identity, Update::Permission)
}

fn begin_update(identity: &str, what: Update) -> Result<Asked, HomebaseError> {
    let at = match what {
        Update::Drive => DRIVE_PATH,
        Update::Permission => APPROVAL_PATH,
    };
    // A random part in the path: the answer carries nothing else only the owner app knows.
    let path = format!("{at}/{}", uuid::Uuid::new_v4().simple());
    let (back, return_url) = Callback::bind(path)?;
    Ok(Asked {
        url: hbsync_homebase::app::update_url(identity, &return_url, what),
        back,
        what,
    })
}

impl Asked {
    /// Open this in the browser.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Sends the browser to `url` once the owner has answered, instead of showing a line of text.
    pub fn return_to(mut self, url: &str) -> Self {
        self.back.return_to = Some(url.to_owned());
        self
    }

    /// Waits up to `timeout` for the owner's answer: `false` unless they gave it. Whether the
    /// drive is there (or the permission held) is for looking up afterwards. Given up as soon
    /// as `cancel` is set.
    pub fn finish_unless(
        self,
        timeout: Duration,
        cancel: &AtomicBool,
    ) -> Result<bool, HomebaseError> {
        self.finish_with(timeout, cancel, None)
    }

    /// [`Asked::finish_unless`], the owner's answer taken also from the address the
    /// browser ended on, pasted ([`Login::finish_with`]): this trip's by its path alone.
    pub(crate) fn finish_with(
        self,
        timeout: Duration,
        cancel: &AtomicBool,
        paste: Option<Paste<'_>>,
    ) -> Result<bool, HomebaseError> {
        let (query, browser) = self.back.wait(timeout, cancel, paste, |_| true)?;
        // Given comes back without a status; cancelled (or anything else) with one.
        let given = !query.contains_key("status");
        let outcome = match (self.what, given) {
            (Update::Drive, true) => Ok("goes on setting up"),
            (Update::Drive, false) => Err(format!("{APP_NAME} didn't get its drive back.")),
            (Update::Permission, true) => Ok("syncs what others share with you"),
            (Update::Permission, false) => Err(format!(
                "{APP_NAME} wasn't given the permission to read what others share with you."
            )),
        };
        self.back.answer(browser, outcome);
        Ok(given)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    type R = Result<(), Box<dyn std::error::Error>>;

    /// The URL the owner app sends the browser back to, from the page's `return`.
    fn return_url(page: &str) -> Result<String, Box<dyn std::error::Error>> {
        let (_, query) = page.split_once('?').ok_or("no query")?;
        let mut params = local_http::parse_form(query.as_bytes());
        Ok(params.remove("return").ok_or("no return")?)
    }

    /// The browser coming back to `url` with `query`: the answer it gets.
    fn come_back(url: &str, query: &str) -> std::io::Result<String> {
        let rest = url.trim_start_matches("http://");
        let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
        let mut s = TcpStream::connect(host.replace("localhost", "127.0.0.1"))?;
        s.set_read_timeout(Some(Duration::from_secs(10)))?;
        write!(s, "GET /{path}{query} HTTP/1.1\r\nHost: {host}\r\n\r\n")?;
        let mut out = String::new();
        s.read_to_string(&mut out)?;
        Ok(out)
    }

    /// The address a browser on another computer ended on, pasted, is the owner's answer as
    /// the browser's request is: the whole address, or from its path on; a line that isn't it
    /// is said and waited on, an empty one passed over.
    #[test]
    fn the_address_the_browser_ended_on_can_be_pasted() -> R {
        use std::cell::RefCell;
        for (query, given) in [("", true), ("?status=canceled#top", false)] {
            let restore = begin_restore("anna.example.com")?;
            let back = return_url(restore.url())?;
            let path = back.trim_start_matches("http://localhost");
            let path = &path[path.find('/').ok_or("no path")?..];
            // (As typed, the last first; `None`: nothing more just then.)
            let lines = RefCell::new(vec![
                Some(format!("  {path}{query}\n")),
                None,
                Some("http://localhost:1/drive-callback/another".to_owned()),
                Some(String::new()),
                Some("what was that again".to_owned()),
                None,
            ]);
            let said = RefCell::new(Vec::new());
            let paste = Paste {
                typed: &|| lines.borrow_mut().pop().flatten(),
                say: &|why| said.borrow_mut().push(why.to_owned()),
            };
            assert_eq!(
                restore.finish_with(
                    Duration::from_secs(10),
                    &AtomicBool::new(false),
                    Some(paste)
                )?,
                given
            );
            // (Said once for what was typed at one go.)
            assert_eq!(*said.borrow(), [NOT_THE_ADDRESS]);
            assert!(lines.borrow().is_empty());
        }
        // The whole address, whatever its host says: it is the path that is this one's.
        let restore = begin_restore("anna.example.com")?;
        let back = return_url(restore.url())?.replace("localhost", "127.0.0.1");
        let typed = RefCell::new(vec![Some(back), None]);
        let paste = Paste {
            typed: &|| typed.borrow_mut().pop().flatten(),
            say: &|_| {},
        };
        assert!(restore.finish_with(
            Duration::from_secs(10),
            &AtomicBool::new(false),
            Some(paste)
        )?);
        Ok(())
    }

    /// What counts as the address the browser was sent back to: this callback's path, on any
    /// host and port, the whole address or from its path on.
    #[test]
    fn the_pasted_address_is_told_by_its_path() -> R {
        let (back, url) = Callback::bind("/back/1".to_owned())?;
        let query = |line: &str| back.pasted(line).map(|q| q.get("a").cloned());
        let a = Some(Some("1".to_owned()));
        for taken in [
            format!("{url}?a=1"),
            "http://localhost:9/back/1?a=1#top".to_owned(),
            "https://example.com/back/1?a=1".to_owned(),
            "/back/1?a=1".to_owned(),
        ] {
            assert_eq!(query(&taken), a, "{taken}");
        }
        assert_eq!(query("/back/1"), Some(None), "no query");
        for not_it in [
            "?a=1",
            "a=1",
            "localhost:9/back/1?a=1",
            "http://localhost:9/back/2?a=1",
            "http://localhost:9/back/1/?a=1",
            "http://localhost:9//back/1?a=1",
            "http://localhost:9",
            "http://localhost:9/?x=/back/1?a=1",
        ] {
            assert_eq!(query(not_it), None, "{not_it}");
        }
        Ok(())
    }

    /// While an address can be pasted, the browser coming back by itself (a port forwarded)
    /// is the answer as before, and lines typed before the trip began are none of its.
    #[test]
    fn the_browser_comes_back_while_an_address_can_be_pasted() -> R {
        use std::cell::RefCell;
        let restore = begin_restore("anna.example.com")?;
        let back = return_url(restore.url())?;
        // (As typed before: this trip's own address among them, as if pasted for an earlier one.)
        let early = RefCell::new(vec![
            Some(format!("{back}?status=canceled")),
            Some("x".to_owned()),
        ]);
        let said = RefCell::new(0);
        let paste = Paste {
            typed: &|| early.borrow_mut().pop().flatten(),
            say: &|_| *said.borrow_mut() += 1,
        };
        let browser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            come_back(&back, "")
        });
        let never = AtomicBool::new(false);
        assert!(restore.finish_with(Duration::from_secs(10), &never, Some(paste))?);
        assert!(
            browser
                .join()
                .map_err(|_| "browser")??
                .contains("goes on setting up")
        );
        assert_eq!(*said.borrow(), 0);
        Ok(())
    }

    /// A login takes the owner's answer from a pasted address too: the address of another
    /// login is said and waited on, and the owner's refusal in this one's ends the login, as
    /// the browser's request would.
    #[test]
    fn a_login_takes_the_owners_answer_from_a_pasted_address() -> R {
        use std::cell::RefCell;
        let login = begin("anna.example.com", "Filesync on a test")?;
        let (_, query) = login.authorize_url().split_once('?').ok_or("no query")?;
        let asked = local_http::parse_form(query.as_bytes());
        let (state, back) = (&asked["state"], &asked["redirect_uri"]);
        // (As typed, the last first.)
        let lines = RefCell::new(vec![
            Some(format!(
                "{back}?state={state}&error=access_denied&error_description=no"
            )),
            None,
            // (This login's, a part of it lost: waited on too, not the end of the login.)
            Some(format!("{back}?state={state}&public_key=x")),
            Some(format!("{back}?state=another&public_key=x&salt=eQ==")),
            None,
        ]);
        let said = RefCell::new(Vec::new());
        let paste = Paste {
            typed: &|| lines.borrow_mut().pop().flatten(),
            say: &|why| said.borrow_mut().push(why.to_owned()),
        };
        let refused = login.finish_with(
            Duration::from_secs(10),
            &AtomicBool::new(false),
            Some(paste),
        );
        assert!(
            matches!(&refused, Err(HomebaseError::Denied(why)) if why.contains("access_denied")),
            "{refused:?}"
        );
        let said = said.borrow();
        assert_eq!(*said, [NOT_THE_ADDRESS]);
        Ok(())
    }

    /// The owner's answer to giving the drive back: given unless they cancelled; the browser
    /// sent on to the page that follows along, with why when not.
    #[test]
    fn giving_the_drive_back_waits_for_the_owner() -> R {
        for (query, given) in [("", true), ("?status=canceled", false)] {
            let restore = begin_restore("anna.example.com")?.return_to("http://x/done");
            let back = return_url(restore.url())?;
            let browser = std::thread::spawn(move || come_back(&back, query));
            let answer = restore.finish_unless(Duration::from_secs(10), &AtomicBool::new(false))?;
            assert_eq!(answer, given);
            let page = browser.join().map_err(|_| "browser")??;
            let expected = if given {
                "Location: http://x/done\r\n"
            } else {
                "Location: http://x/done?error="
            };
            assert!(page.contains(expected), "{page}");
        }
        Ok(())
    }

    /// Asking the owner to let the app read what others share with them is its own trip on
    /// the app-update page: for the permission alone (no drive), back on a path of its own,
    /// given unless they cancelled, and the browser told which in the app's words.
    #[test]
    fn approving_what_others_share_waits_for_the_owner() -> R {
        for (query, given) in [("", true), ("?status=canceled", false)] {
            let ask = begin_approval("anna.example.com")?;
            assert!(
                ask.url()
                    .starts_with("https://anna.example.com/owner/appupdate?")
            );
            assert!(ask.url().contains("p=305") && !ask.url().contains("&d="));
            let back = return_url(ask.url())?;
            assert!(back.contains("/approval-callback/"), "{back}");
            let browser = std::thread::spawn(move || come_back(&back, query));
            let answer = ask.finish_unless(Duration::from_secs(10), &AtomicBool::new(false))?;
            assert_eq!(answer, given);
            let page = browser.join().map_err(|_| "browser")??;
            let said = if given {
                "syncs what others share with you"
            } else {
                "wasn't given the permission"
            };
            assert!(page.contains(said), "{page}");
        }
        // A drive's answer isn't an approval's: each trip is told by its own path.
        let (restore, ask) = (begin_restore("a.example")?, begin_approval("a.example")?);
        assert!(return_url(restore.url())?.contains("/drive-callback/"));
        assert!(!ask.back.pasted(&return_url(restore.url())?).is_some());
        Ok(())
    }
}
