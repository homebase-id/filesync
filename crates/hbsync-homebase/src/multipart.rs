//! A streaming `multipart/form-data` body with an exact length, for uploads: a payload part
//! streams from a reader of known size, so memory stays flat and the request carries a
//! `Content-Length` (no chunked encoding).

use std::io::{self, Cursor, Read};

/// One form part.
pub struct Part<'a> {
    name: &'static str,
    filename: Option<&'static str>,
    content_type: &'static str,
    body: PartBody<'a>,
}

enum PartBody<'a> {
    Bytes(Vec<u8>),
    Reader {
        reader: &'a mut dyn Read,
        size: u64,
    },
    Later {
        make: Box<dyn FnOnce() -> Vec<u8> + 'a>,
        size: u64,
    },
}

impl std::fmt::Debug for Part<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Part")
            .field("name", &self.name)
            .field("filename", &self.filename)
            .field("len", &self.body_len())
            .finish()
    }
}

impl<'a> Part<'a> {
    pub fn bytes(name: &'static str, content_type: &'static str, bytes: Vec<u8>) -> Self {
        Part {
            name,
            filename: None,
            content_type,
            body: PartBody::Bytes(bytes),
        }
    }

    /// A part streamed from `reader`, which must yield `size` bytes.
    pub fn reader(
        name: &'static str,
        filename: &'static str,
        content_type: &'static str,
        reader: &'a mut dyn Read,
        size: u64,
    ) -> Self {
        Part {
            name,
            filename: Some(filename),
            content_type,
            body: PartBody::Reader { reader, size },
        }
    }

    /// A part made by `make` when the body reaches it, after the parts before it were read
    /// (a hash of what they sent). It must make `size` bytes: the length is sent first.
    pub fn later(
        name: &'static str,
        filename: &'static str,
        content_type: &'static str,
        size: u64,
        make: impl FnOnce() -> Vec<u8> + 'a,
    ) -> Self {
        Part {
            name,
            filename: Some(filename),
            content_type,
            body: PartBody::Later {
                make: Box::new(make),
                size,
            },
        }
    }

    fn body_len(&self) -> u64 {
        match &self.body {
            PartBody::Bytes(b) => b.len() as u64,
            PartBody::Reader { size, .. } | PartBody::Later { size, .. } => *size,
        }
    }

    fn head(&self, boundary: &str) -> Vec<u8> {
        let mut head = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"{}\"",
            self.name
        );
        if let Some(filename) = &self.filename {
            head.push_str(&format!("; filename=\"{filename}\""));
        }
        head.push_str(&format!("\r\nContent-Type: {}\r\n\r\n", self.content_type));
        head.into_bytes()
    }
}

/// The parts of a form, in order.
#[derive(Debug)]
pub struct Multipart<'a> {
    boundary: String,
    parts: Vec<Part<'a>>,
}

impl<'a> Multipart<'a> {
    /// `boundary` must not occur in any part (a random one: 32 hex characters).
    pub fn new(boundary: String, parts: Vec<Part<'a>>) -> Self {
        Multipart { boundary, parts }
    }

    pub fn content_type(&self) -> String {
        format!("multipart/form-data; boundary={}", self.boundary)
    }

    /// The exact length of the body.
    pub fn content_length(&self) -> u64 {
        let parts: u64 = self
            .parts
            .iter()
            .map(|p| p.head(&self.boundary).len() as u64 + p.body_len() + 2)
            .sum();
        parts + self.tail().len() as u64
    }

    fn tail(&self) -> Vec<u8> {
        format!("--{}--\r\n", self.boundary).into_bytes()
    }

    /// The body as one reader. A streamed part that ends early is an error ([`SourceEnded`]);
    /// one that has more is cut at its size.
    pub fn into_reader(self) -> impl Read + 'a {
        let tail = Cursor::new(self.tail());
        let mut body: Box<dyn Read + 'a> = Box::new(io::empty());
        for part in self.parts {
            let head = Cursor::new(part.head(&self.boundary));
            let content: Box<dyn Read + 'a> = match part.body {
                PartBody::Bytes(b) => Box::new(Cursor::new(b)),
                PartBody::Reader { reader, size } => Box::new(Exact {
                    inner: reader,
                    remaining: size,
                }),
                PartBody::Later { make, size } => Box::new(Later {
                    make: Some(make),
                    size,
                    made: Cursor::new(Vec::new()),
                }),
            };
            body = Box::new(body.chain(head).chain(content).chain(&b"\r\n"[..]));
        }
        body.chain(tail)
    }
}

/// The bytes of a [`Part::later`], made at the first read.
struct Later<'a> {
    make: Option<Box<dyn FnOnce() -> Vec<u8> + 'a>>,
    size: u64,
    made: Cursor<Vec<u8>>,
}

impl Read for Later<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if let Some(make) = self.make.take() {
            let made = make();
            if made.len() as u64 != self.size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "a part of {} bytes was announced as {}",
                        made.len(),
                        self.size
                    ),
                ));
            }
            self.made = Cursor::new(made);
        }
        self.made.read(buf)
    }
}

/// A streamed part's source ended before its announced size: it changed since then.
#[derive(Debug)]
pub struct SourceEnded {
    pub missing: u64,
}

impl std::fmt::Display for SourceEnded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "upload source ended {} bytes early", self.missing)
    }
}

impl std::error::Error for SourceEnded {}

/// Exactly `remaining` bytes of `inner`.
struct Exact<'a> {
    inner: &'a mut dyn Read,
    remaining: u64,
}

impl Read for Exact<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 || buf.is_empty() {
            return Ok(0);
        }
        let want = buf
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let n = self.inner.read(&mut buf[..want])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                SourceEnded {
                    missing: self.remaining,
                },
            ));
        }
        self.remaining -= n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_matches_its_length_and_the_format() -> io::Result<()> {
        let mut payload: &[u8] = b"payload bytes";
        let form = Multipart::new(
            "XyZ".into(),
            vec![
                Part::bytes("instructions", "application/json", b"{}".to_vec()),
                Part::bytes("metadata", "application/octet-stream", vec![0, 1, 2]),
                Part::reader(
                    "payload",
                    "sync_data",
                    "application/octet-stream",
                    &mut payload,
                    13,
                ),
            ],
        );
        let len = form.content_length();
        let mut body = Vec::new();
        form.into_reader().read_to_end(&mut body)?;
        assert_eq!(body.len() as u64, len);
        let expected: &[u8] = b"--XyZ\r\n\
            Content-Disposition: form-data; name=\"instructions\"\r\n\
            Content-Type: application/json\r\n\r\n{}\r\n\
            --XyZ\r\n\
            Content-Disposition: form-data; name=\"metadata\"\r\n\
            Content-Type: application/octet-stream\r\n\r\n\x00\x01\x02\r\n\
            --XyZ\r\n\
            Content-Disposition: form-data; name=\"payload\"; filename=\"sync_data\"\r\n\
            Content-Type: application/octet-stream\r\n\r\npayload bytes\r\n\
            --XyZ--\r\n";
        assert_eq!(
            String::from_utf8_lossy(&body),
            String::from_utf8_lossy(expected)
        );
        Ok(())
    }

    #[test]
    fn a_short_source_fails_and_a_long_one_is_cut() {
        let mut short: &[u8] = b"abc";
        let form = Multipart::new(
            "b".into(),
            vec![Part::reader("payload", "k", "x", &mut short, 5)],
        );
        let err = form.into_reader().read_to_end(&mut Vec::new());
        assert_eq!(err.map_err(|e| e.kind()), Err(io::ErrorKind::UnexpectedEof));

        let mut long: &[u8] = b"abcdef";
        let form = Multipart::new(
            "b".into(),
            vec![Part::reader("payload", "k", "x", &mut long, 2)],
        );
        let len = form.content_length();
        let mut body = Vec::new();
        assert!(form.into_reader().read_to_end(&mut body).is_ok());
        assert_eq!(body.len() as u64, len);
        assert!(String::from_utf8_lossy(&body).contains("\r\n\r\nab\r\n--b--"));
    }

    #[test]
    fn a_later_part_is_made_after_the_parts_before_it_were_read() -> io::Result<()> {
        let seen = std::cell::Cell::new(0u64);
        let mut payload = Counted {
            inner: &b"abc"[..],
            seen: &seen,
        };
        let form = Multipart::new(
            "b".into(),
            vec![
                Part::reader("payload", "k", "x", &mut payload, 3),
                Part::later("payloadHash", "k", "application/json", 2, || {
                    seen.get().to_string().into_bytes().repeat(2)
                }),
            ],
        );
        let len = form.content_length();
        let mut body = Vec::new();
        form.into_reader().read_to_end(&mut body)?;
        assert_eq!(body.len() as u64, len);
        assert!(String::from_utf8_lossy(&body).contains("\r\n\r\n33\r\n--b--"));

        let wrong = Multipart::new("b".into(), vec![Part::later("h", "k", "x", 5, Vec::new)]);
        let err = wrong.into_reader().read_to_end(&mut Vec::new());
        assert_eq!(err.map_err(|e| e.kind()), Err(io::ErrorKind::InvalidData));
        Ok(())
    }

    /// Counts the bytes read from `inner`.
    struct Counted<'a, R> {
        inner: R,
        seen: &'a std::cell::Cell<u64>,
    }

    impl<R: Read> Read for Counted<'_, R> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = self.inner.read(buf)?;
            self.seen.set(self.seen.get() + n as u64);
            Ok(n)
        }
    }
}
