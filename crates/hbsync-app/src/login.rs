//! Logging this device in to an identity in the browser: the YouAuth exchange
//! ([`hbsync_homebase::youauth`]) with the page on localhost the identity sends the browser
//! back to.

use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hbsync_homebase::{Credentials, HomebaseError, Pending};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

use crate::local_http;

const CALLBACK_PATH: &str = "/authorization-code-callback";

/// A login waiting for the browser to come back.
#[derive(Debug)]
pub struct Login {
    pending: Pending,
    listener: TcpListener,
    /// Where the browser goes once the identity is back (`?error=` added on failure); else it
    /// shows a line of text.
    return_to: Option<String>,
}

/// Starts logging in to `identity` (a domain such as `frodo.dotyou.cloud`); `friendly_name`
/// names the device in the owner's list.
pub fn begin(identity: &str, friendly_name: &str) -> Result<Login, HomebaseError> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    let redirect_uri = format!("http://localhost:{port}{CALLBACK_PATH}");
    Ok(Login {
        pending: hbsync_homebase::youauth::begin(identity, friendly_name, &redirect_uri)?,
        listener,
        return_to: None,
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
        self.return_to = Some(url.to_owned());
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
            let Some(request) = request.filter(|r| r.method == "GET" && r.path == CALLBACK_PATH)
            else {
                let _ = local_http::message(&mut stream, 404, "Not here.");
                continue;
            };
            let result = self.pending.complete(&request.query);
            match (&self.return_to, &result) {
                (Some(url), Ok(_)) => local_http::redirect(&mut stream, url)?,
                (Some(url), Err(e)) => {
                    let why = utf8_percent_encode(&e.to_string(), NON_ALPHANUMERIC).to_string();
                    let sep = if url.contains('?') { '&' } else { '?' };
                    local_http::redirect(&mut stream, &format!("{url}{sep}error={why}"))?;
                }
                (None, Ok(_)) => local_http::message(
                    &mut stream,
                    200,
                    "Homebase Filesync is connected. You can close this window.",
                )?,
                (None, Err(e)) => local_http::message(
                    &mut stream,
                    200,
                    &format!("Homebase Filesync could not connect: {e}"),
                )?,
            }
            return result;
        }
    }
}
