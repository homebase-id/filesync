//! Trips of the browser to the identity's owner app that come back to a page on localhost:
//! logging this device in (the YouAuth exchange, [`hbsync_homebase::youauth`]), and giving the
//! app its drive again.

use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hbsync_homebase::{Credentials, HomebaseError, Pending};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

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

impl Callback {
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

    /// Waits up to `timeout` for the browser's request, given up as soon as `cancel` is set.
    fn wait(
        &self,
        timeout: Duration,
        cancel: &AtomicBool,
    ) -> Result<(TcpStream, Request), HomebaseError> {
        let deadline = Instant::now() + timeout;
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
                Some(request) => return Ok((stream, request)),
                None => {
                    let _ = local_http::message(&mut stream, 404, "Not here.");
                }
            }
        }
    }
}

impl Callback {
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
                local_http::message(stream, 200, outcome.as_deref().unwrap_or_else(|w| w))
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
        let (mut stream, request) = self.back.wait(timeout, cancel)?;
        let result = self.pending.complete(&request.query);
        let outcome = match &result {
            Ok(_) => Ok("Homebase Filesync is connected. You can close this window."),
            Err(e) => Err(format!("Homebase Filesync could not connect: {e}")),
        };
        self.back.answer(&mut stream, outcome);
        result
    }
}

/// The owner giving the app its drive again ([`hbsync_homebase::app::restore_drive_url`]),
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
        url: hbsync_homebase::app::restore_drive_url(identity, &return_url),
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

    /// Waits up to `timeout` for the owner's answer: `false` when they said no. Whether the
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
        let (mut stream, request) = self.back.wait(timeout, cancel)?;
        let given = request.query.get("status").map(String::as_str) != Some("canceled");
        let outcome = if given {
            Ok("Homebase Filesync goes on setting up. You can close this window.")
        } else {
            Err("Homebase Filesync didn't get its drive back.".to_owned())
        };
        self.back.answer(&mut stream, outcome);
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
