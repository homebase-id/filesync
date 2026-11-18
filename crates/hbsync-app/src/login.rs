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

use hbsync_homebase::{Credentials, HomebaseError, Pending};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

use crate::APP_NAME;
use crate::local_http::{self, Request};

const CALLBACK_PATH: &str = "/authorization-code-callback";
const DRIVE_PATH: &str = "/drive-callback";

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
/// computer ended on ([`Login::finish_or_pasted`]).
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

/// The owner's answer, as it came back.
enum Back {
    /// The browser came to this computer: it waits for an answer.
    Browser(TcpStream, Request),
    /// The address it ended on elsewhere was pasted: its query.
    Pasted(HashMap<String, String>),
}

impl Callback {
    /// The query of the address `line`, if that is the one the browser was sent back to
    /// (this callback's path, on any host and port: the answer in it says whose it is);
    /// `None` for an empty line, and why not for anything else. An address alone, or from its
    /// path on, or its query alone.
    fn pasted(&self, line: &str) -> Result<Option<HashMap<String, String>>, String> {
        let line = line.trim();
        if line.is_empty() {
            return Ok(None);
        }
        let (address, query) = line.split_once('?').unwrap_or((line, ""));
        let query = query.split_once('#').map_or(query, |(query, _)| query);
        // (After the host, where there is one.)
        let path = match address.split_once("://") {
            Some((_, rest)) => rest.find('/').map_or("", |at| &rest[at..]),
            None => address,
        };
        if !path.is_empty() && path != self.path {
            return Err(
                "That isn't the address the browser ended on after you approved: paste the \
                 whole address, starting with http://localhost"
                    .to_owned(),
            );
        }
        Ok(Some(local_http::parse_form(query.as_bytes())))
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

    /// Waits till `deadline` for the browser's request, or for its address pasted (`paste`),
    /// given up as soon as `cancel` is set.
    fn wait(
        &self,
        deadline: Instant,
        cancel: &AtomicBool,
        paste: Option<Paste<'_>>,
    ) -> Result<Back, HomebaseError> {
        self.listener.set_nonblocking(true)?;
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
                    if let Some(paste) = paste
                        && let Some(line) = (paste.typed)()
                    {
                        match self.pasted(&line) {
                            Ok(Some(query)) => return Ok(Back::Pasted(query)),
                            Ok(None) => {}
                            Err(why) => (paste.say)(&why),
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
                Some(request) => return Ok(Back::Browser(stream, request)),
                None => {
                    let _ = local_http::message(&mut stream, 404, "Not here.");
                }
            }
        }
    }

    /// Answers the browser that came back: sent on to `return_to` (with `?error=<why>` on
    /// failure), else a line of text. A browser gone meanwhile (the tab closed) changes
    /// nothing: the owner's answer came.
    fn answer(&self, stream: &mut TcpStream, outcome: Result<&str, String>) {
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

    /// Waits up to `timeout` for the browser's callback, then exchanges it for credentials.
    pub fn finish(self, timeout: Duration) -> Result<Credentials, HomebaseError> {
        self.finish_unless(timeout, &AtomicBool::new(false))
    }

    /// [`Login::finish`], given up as soon as `cancel` is set (the user started over).
    pub fn finish_unless(
        self,
        timeout: Duration,
        cancel: &AtomicBool,
    ) -> Result<Credentials, HomebaseError> {
        self.finish_with(timeout, cancel, None)
    }

    /// [`Login::finish`], the owner's answer taken also from the address the browser ended
    /// on, pasted (`paste`): for a browser on another computer, which can't come back to
    /// this one. An address that isn't this login's answer is said, and waited on.
    pub fn finish_or_pasted(
        self,
        timeout: Duration,
        paste: Paste<'_>,
    ) -> Result<Credentials, HomebaseError> {
        self.finish_with(timeout, &AtomicBool::new(false), Some(paste))
    }

    fn finish_with(
        self,
        timeout: Duration,
        cancel: &AtomicBool,
        paste: Option<Paste<'_>>,
    ) -> Result<Credentials, HomebaseError> {
        let deadline = Instant::now() + timeout;
        loop {
            let query = match self.back.wait(deadline, cancel, paste)? {
                Back::Browser(mut stream, request) => {
                    let result = self.pending.complete(&request.query);
                    let outcome = match &result {
                        Ok(_) => Ok("is connected"),
                        Err(e) => Err(format!("{APP_NAME} could not connect: {e}")),
                    };
                    self.back.answer(&mut stream, outcome);
                    return result;
                }
                Back::Pasted(query) => query,
            };
            match (self.pending.complete(&query), paste) {
                // Not this login's answer (another try's address, a part of it cut off).
                (Err(HomebaseError::Protocol(why)), Some(paste)) => (paste.say)(&format!(
                    "That address doesn't answer this login ({why}). Paste the one the \
                     browser ended on."
                )),
                (result, _) => return result,
            }
        }
    }
}

/// The owner giving the app its drive again ([`hbsync_homebase::app::Update::Drive`]),
/// waiting for the browser to come back.
#[derive(Debug)]
pub struct DriveRestore {
    url: String,
    back: Callback,
}

/// Starts asking the owner of `identity` to give the app its drive again.
pub(crate) fn begin_restore(identity: &str) -> Result<DriveRestore, HomebaseError> {
    // A random part in the path: the answer carries nothing else only the owner app knows.
    let path = format!("{DRIVE_PATH}/{}", uuid::Uuid::new_v4().simple());
    let (back, return_url) = Callback::bind(path)?;
    Ok(DriveRestore {
        url: hbsync_homebase::app::update_url(
            identity,
            &return_url,
            hbsync_homebase::app::Update::Drive,
        ),
        back,
    })
}

impl DriveRestore {
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
    /// drive is there is for looking up afterwards.
    pub fn finish(self, timeout: Duration) -> Result<bool, HomebaseError> {
        self.finish_unless(timeout, &AtomicBool::new(false))
    }

    /// [`DriveRestore::finish`], given up as soon as `cancel` is set.
    pub fn finish_unless(
        self,
        timeout: Duration,
        cancel: &AtomicBool,
    ) -> Result<bool, HomebaseError> {
        self.finish_with(timeout, cancel, None)
    }

    /// [`DriveRestore::finish`], the owner's answer taken also from the address the browser
    /// ended on, pasted ([`Login::finish_or_pasted`]).
    pub fn finish_or_pasted(
        self,
        timeout: Duration,
        paste: Paste<'_>,
    ) -> Result<bool, HomebaseError> {
        self.finish_with(timeout, &AtomicBool::new(false), Some(paste))
    }

    fn finish_with(
        self,
        timeout: Duration,
        cancel: &AtomicBool,
        paste: Option<Paste<'_>>,
    ) -> Result<bool, HomebaseError> {
        // Given comes back without a status; cancelled (or anything else) with one.
        let given = |query: &HashMap<String, String>| !query.contains_key("status");
        match self.back.wait(Instant::now() + timeout, cancel, paste)? {
            Back::Browser(mut stream, request) => {
                let given = given(&request.query);
                let outcome = if given {
                    Ok("goes on setting up")
                } else {
                    Err(format!("{APP_NAME} didn't get its drive back."))
                };
                self.back.answer(&mut stream, outcome);
                Ok(given)
            }
            Back::Pasted(query) => Ok(given(&query)),
        }
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
            // (As typed, the last first.)
            let lines = RefCell::new(vec![
                format!("  {path}{query}\n"),
                "http://localhost:1/drive-callback/another".to_owned(),
                String::new(),
                "what was that again".to_owned(),
            ]);
            let said = RefCell::new(Vec::new());
            let paste = Paste {
                typed: &|| lines.borrow_mut().pop(),
                say: &|why| said.borrow_mut().push(why.to_owned()),
            };
            assert_eq!(
                restore.finish_or_pasted(Duration::from_secs(10), paste)?,
                given
            );
            assert_eq!(said.borrow().len(), 2, "{said:?}");
            assert!(lines.borrow().is_empty());
        }
        // The whole address, whatever its host says: it is the path that is this one's.
        let restore = begin_restore("anna.example.com")?;
        let back = return_url(restore.url())?.replace("localhost", "127.0.0.1");
        let paste = Paste {
            typed: &|| Some(back.clone()),
            say: &|_| {},
        };
        assert!(restore.finish_or_pasted(Duration::from_secs(10), paste)?);
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
            format!("{back}?state={state}&error=access_denied&error_description=no"),
            format!("{back}?state=another&public_key=x&salt=y"),
        ]);
        let said = RefCell::new(Vec::new());
        let paste = Paste {
            typed: &|| lines.borrow_mut().pop(),
            say: &|why| said.borrow_mut().push(why.to_owned()),
        };
        let refused = login.finish_or_pasted(Duration::from_secs(10), paste);
        assert!(
            matches!(&refused, Err(HomebaseError::Denied(why)) if why.contains("access_denied")),
            "{refused:?}"
        );
        let said = said.borrow();
        assert!(
            said.len() == 1 && said[0].contains("doesn't answer this login"),
            "{said:?}"
        );
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
            let answer = restore.finish(Duration::from_secs(10))?;
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
}
