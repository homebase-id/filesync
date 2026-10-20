//! The little HTTP the client serves on localhost: the YouAuth callback and the tray's pages.
//! One request per connection, answered and closed; nothing is cached or sent on as a referrer.

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// A request larger than this (line, headers and body) isn't read.
pub(crate) const MAX_REQUEST: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub method: String,
    /// Without the query.
    pub path: String,
    pub query: HashMap<String, String>,
    /// The body, read as a form (`a=1&b=x+y`).
    pub form: HashMap<String, String>,
}

/// Reads one request; `None` for anything that isn't HTTP (a browser trying TLS first, as
/// Firefox's HTTPS-First does) or is too large.
pub fn read_request(stream: &mut TcpStream) -> io::Result<Option<Request>> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut reader = BufReader::new(stream.take(MAX_REQUEST as u64));
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line)?;
    let line = String::from_utf8_lossy(&line);
    let mut parts = line.split_whitespace();
    let (Some(method), Some(target), Some(version)) = (parts.next(), parts.next(), parts.next())
    else {
        return Ok(None);
    };
    if !version.starts_with("HTTP/") {
        return Ok(None);
    }
    let mut length = 0;
    loop {
        let mut header = Vec::new();
        if reader.read_until(b'\n', &mut header)? == 0 {
            return Ok(None);
        }
        let header = String::from_utf8_lossy(&header);
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
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    Ok(Some(Request {
        method: method.to_owned(),
        path: path.to_owned(),
        query: parse_form(query.as_bytes()),
        form: parse_form(&body),
    }))
}

/// `a=1&b=x%20y` (also `+` for spaces); bad escapes are read leniently.
pub fn parse_form(s: &[u8]) -> HashMap<String, String> {
    form_urlencoded::parse(s).into_owned().collect()
}

pub fn respond(stream: &mut TcpStream, status: u16, kind: &str, body: &[u8]) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Error",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)
}

pub fn redirect(stream: &mut TcpStream, url: &str) -> io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 303 See Other\r\nLocation: {url}\r\nContent-Length: 0\r\n\
         Cache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nConnection: close\r\n\r\n"
    )
}

/// A plain page saying `text`, escaped.
pub fn message(stream: &mut TcpStream, status: u16, text: &str) -> io::Result<()> {
    let body = format!(
        "<!doctype html><meta charset=utf-8><title>Homebase Filesync</title>\
         <p style=\"font:16px system-ui;margin:3em\">{}</p>",
        escape(text)
    );
    respond(stream, status, "text/html; charset=utf-8", body.as_bytes())
}

/// `s` safe in HTML and XML text and attribute values.
pub fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;

    /// What `sent` reads as, through a real socket.
    fn read(sent: &'static [u8]) -> io::Result<Option<Request>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let client = std::thread::spawn(move || -> io::Result<()> {
            let mut s = TcpStream::connect(addr)?;
            s.write_all(sent)
        });
        let (mut stream, _) = listener.accept()?;
        let request = read_request(&mut stream);
        client.join().expect("client thread")?;
        request
    }

    #[test]
    fn a_form_post_and_a_query() -> io::Result<()> {
        let r = read(
            b"POST /s/setup?x=1&error=a%20b HTTP/1.1\r\nContent-Length: 15\r\n\r\nidentity=a+b%2F",
        )?
        .expect("a request");
        assert_eq!((r.method.as_str(), r.path.as_str()), ("POST", "/s/setup"));
        assert_eq!(r.query["error"], "a b");
        assert_eq!(r.form["identity"], "a b/");
        Ok(())
    }

    #[test]
    fn not_http_is_none() -> io::Result<()> {
        assert_eq!(
            read(b"\x16\x03\x01\x02\x00\x01\x00\x01\xfc\x03\x03\n")?,
            None
        );
        assert_eq!(read(b"GET /\r\n\r\n")?, None);
        Ok(())
    }

    #[test]
    fn forms_read_leniently() {
        let f = parse_form(b"identity=Anna.Example.com%2F&merge=on&x=a+b%");
        assert_eq!(f["identity"], "Anna.Example.com/");
        assert_eq!(f["x"], "a b%");
        assert_eq!(parse_form(b"x=%E2%9C%93")["x"], "✓");
        assert_eq!(
            parse_form("x=%ä".as_bytes())["x"],
            "%ä",
            "no panic in the middle of a character"
        );
    }

    #[test]
    fn text_is_escaped() {
        assert_eq!(
            escape("<a href=\"x\">&"),
            "&lt;a href=&quot;x&quot;&gt;&amp;"
        );
    }
}
