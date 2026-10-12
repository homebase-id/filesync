//! Finder tags: a name and one of Finder's colours, on files and folders.
//!
//! macOS keeps them in the `com.apple.metadata:_kMDItemUserTags` extended attribute (a binary
//! plist array of `"Name\nColour"` strings), which Finder and Spotlight read; Linux in
//! `user.xdg.tags` (comma-separated names, what KDE uses), without colours. Windows has none.
//! What an OS can't hold is left to the drive (sync-semantics §11): [`Tags::fill`].

use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::Path;

use crate::Os;

/// One tag. `colour` is Finder's index: 0 none, 1 grey, 2 green, 3 purple, 4 blue, 5 yellow,
/// 6 red, 7 orange.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Tag {
    pub name: String,
    pub colour: u8,
}

impl Tag {
    pub fn new(name: impl Into<String>, colour: u8) -> Self {
        Tag {
            name: name.into(),
            colour,
        }
    }
}

/// An item's tags: sorted by name, one per name, none with an empty name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Tags(Vec<Tag>);

/// Tags taking more characters than this (names plus a little per tag) aren't synced: the item
/// keeps what the drive has (`appData.content` is capped at 10,240 characters).
pub const MAX_CHARS: usize = 2000;

impl Tags {
    /// Sorted by name; of two with one name, the first is kept.
    pub fn new(tags: impl IntoIterator<Item = Tag>) -> Self {
        let mut by_name: BTreeMap<String, u8> = BTreeMap::new();
        for t in tags {
            if !t.name.is_empty() {
                by_name.entry(t.name).or_insert(t.colour);
            }
        }
        Tags(
            by_name
                .into_iter()
                .map(|(name, colour)| Tag { name, colour })
                .collect(),
        )
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Tag> {
        self.0.iter()
    }

    pub fn get(&self, name: &str) -> Option<&Tag> {
        self.0
            .binary_search_by(|t| t.name.as_str().cmp(name))
            .ok()
            .map(|i| &self.0[i])
    }

    /// Whether they are small enough to sync ([`MAX_CHARS`]).
    pub fn fits(&self) -> bool {
        self.0
            .iter()
            .map(|t| t.name.chars().count() + 16)
            .sum::<usize>()
            <= MAX_CHARS
    }

    /// The three-way merge of a node's tags (sync-semantics §11): what either side added is
    /// in, what either side removed is out; a colour comes from the side that changed it (the
    /// remote's if both did).
    pub fn merge(base: &Tags, local: &Tags, remote: &Tags) -> Tags {
        let names: std::collections::BTreeSet<&str> = base
            .iter()
            .chain(local.iter())
            .chain(remote.iter())
            .map(|t| t.name.as_str())
            .collect();
        let colour = |tags: &Tags, name: &str| tags.get(name).map(|t| t.colour);
        Tags::new(names.into_iter().filter_map(|name| {
            let (b, l, r) = (
                colour(base, name),
                colour(local, name),
                colour(remote, name),
            );
            let kept = match b {
                Some(_) => l.is_some() && r.is_some(),
                None => l.is_some() || r.is_some(),
            };
            if !kept {
                return None;
            }
            let colour = match (l, r) {
                (Some(l), Some(r)) if l != r && Some(r) == b => l,
                (_, Some(r)) => r,
                (l, None) => l.unwrap_or(0),
            };
            Some(Tag::new(name, colour))
        }))
    }

    /// What a device's tags are, from what it `read` (`None`: it can't tell, as on Windows)
    /// and `reference`, the drive's (else the agreed) tags: the reference's tags it can't
    /// hold are kept, and one it read as [`express`] shows a reference tag is that tag (Linux
    /// reads no colours).
    pub fn fill(read: Option<&Tags>, reference: &Tags, os: Os) -> Tags {
        let Some(read) = read else {
            return reference.clone();
        };
        let mut seen: HashSet<&str> = HashSet::new();
        let mut out: Vec<Tag> = Vec::new();
        for r in reference.iter() {
            match express(r, os) {
                None => out.push(r.clone()),
                Some(e) if read.get(&e.name) == Some(&e) => {
                    seen.insert(r.name.as_str());
                    out.push(r.clone());
                }
                Some(_) => {}
            }
        }
        out.extend(
            read.iter()
                .filter(|t| !seen.contains(t.name.as_str()))
                .cloned(),
        );
        Tags::new(out)
    }
}

/// Whether this OS keeps tags at all.
pub const CAN_TAG: bool = cfg!(any(target_os = "macos", target_os = "linux"));

/// How `os` holds `tag`, as reading it back would give it: `None` if it can't.
pub fn express(tag: &Tag, os: Os) -> Option<Tag> {
    match os {
        Os::MacOs if !tag.name.contains('\n') => Some(tag.clone()),
        Os::Linux if !tag.name.contains(',') => Some(Tag::new(tag.name.clone(), 0)),
        _ => None,
    }
}

/// The macOS attribute: a binary plist array of `"Name"` or `"Name\nColour"` strings.
pub const FINDER_ATTR: &str = "com.apple.metadata:_kMDItemUserTags";
/// The Linux attribute: names separated by commas.
pub const XDG_ATTR: &str = "user.xdg.tags";

/// Parses Finder's attribute. A suffix after the last newline that isn't a colour is part of
/// the name.
pub fn decode_finder(bytes: &[u8]) -> Option<Tags> {
    let plist::Value::Array(items) = plist::Value::from_reader(io::Cursor::new(bytes)).ok()? else {
        return None;
    };
    Some(Tags::new(items.iter().filter_map(|v| {
        let s = v.as_string()?;
        let tag = match s.rsplit_once('\n') {
            Some((name, colour)) => match colour.parse::<u8>() {
                Ok(c) => Tag::new(crate::nfc(name), c),
                Err(_) => Tag::new(crate::nfc(s), 0),
            },
            None => Tag::new(crate::nfc(s), 0),
        };
        Some(tag)
    })))
}

/// Finder's attribute for `tags`, of those it can hold.
pub fn encode_finder(tags: &Tags) -> Vec<u8> {
    let items = tags
        .iter()
        .filter_map(|t| express(t, Os::MacOs))
        .map(|t| {
            plist::Value::String(if t.colour == 0 {
                t.name
            } else {
                format!("{}\n{}", t.name, t.colour)
            })
        })
        .collect();
    let mut out = Vec::new();
    // Writing a plist of strings to memory can't fail.
    let _ = plist::Value::Array(items).to_writer_binary(&mut out);
    out
}

pub fn decode_xdg(bytes: &[u8]) -> Tags {
    let text = String::from_utf8_lossy(bytes);
    Tags::new(text.split(',').map(|n| Tag::new(crate::nfc(n), 0)))
}

/// The Linux attribute for `tags`, of those it can hold.
pub fn encode_xdg(tags: &Tags) -> Vec<u8> {
    tags.iter()
        .filter_map(|t| express(t, Os::Linux))
        .map(|t| t.name)
        .collect::<Vec<_>>()
        .join(",")
        .into_bytes()
}

/// The tags of the item at `path` (a link itself, never its target). `None` if they can't be
/// told: no tags on this OS or file system, unreadable, or too many to sync ([`Tags::fits`]).
pub fn read(path: &Path) -> Option<Tags> {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        #[cfg(target_os = "macos")]
        let tags = match xattr::get(path, FINDER_ATTR).ok()? {
            Some(bytes) => decode_finder(&bytes)?,
            None => Tags::default(),
        };
        #[cfg(target_os = "linux")]
        let tags = match xattr::get(path, XDG_ATTR).ok()? {
            Some(bytes) => decode_xdg(&bytes),
            None => Tags::default(),
        };
        tags.fits().then_some(tags)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = path;
        None
    }
}

/// Gives the item at `path` these tags (those this OS can hold), then reads them back. Where
/// tags can't be kept ([`CAN_TAG`], a file system without them) it does nothing and returns
/// `None`, as [`read`] would.
pub fn write(path: &Path, tags: &Tags) -> io::Result<Option<Tags>> {
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    {
        #[cfg(target_os = "macos")]
        let (attr, value) = (FINDER_ATTR, encode_finder(tags));
        #[cfg(target_os = "linux")]
        let (attr, value) = (XDG_ATTR, encode_xdg(tags));
        let held = tags.iter().any(|t| express(t, Os::current()).is_some());
        let done = if held {
            xattr::set(path, attr, &value)
        } else {
            xattr::remove(path, attr)
        };
        match done {
            Err(e) if e.kind() == io::ErrorKind::Unsupported => return Ok(None),
            done => done?,
        }
        Ok(read(path))
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (path, tags);
        Ok(None)
    }
}

/// Extended attributes of a path itself (links not followed).
#[cfg(any(target_os = "macos", target_os = "linux"))]
mod xattr {
    use std::ffi::CString;
    use std::io;
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    fn cstrings(path: &Path, name: &str) -> io::Result<(CString, CString)> {
        Ok((
            CString::new(path.as_os_str().as_bytes())?,
            CString::new(name)?,
        ))
    }

    /// "No such attribute".
    #[cfg(target_os = "macos")]
    const NO_ATTR: i32 = libc::ENOATTR;
    #[cfg(target_os = "linux")]
    const NO_ATTR: i32 = libc::ENODATA;

    /// The error of a failed call; a file system without attributes is `Unsupported`.
    fn last_error() -> io::Error {
        let e = io::Error::last_os_error();
        match e.raw_os_error() {
            Some(libc::ENOTSUP) => io::Error::new(io::ErrorKind::Unsupported, e),
            // The same number on Linux.
            #[cfg(target_os = "macos")]
            Some(libc::EOPNOTSUPP) => io::Error::new(io::ErrorKind::Unsupported, e),
            _ => e,
        }
    }

    unsafe fn get_raw(
        path: &CString,
        name: &CString,
        buf: *mut libc::c_void,
        size: usize,
    ) -> isize {
        // SAFETY: the caller passes valid C strings and a buffer of `size` bytes (or null, 0).
        unsafe {
            #[cfg(target_os = "macos")]
            return libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                buf,
                size,
                0,
                libc::XATTR_NOFOLLOW,
            );
            #[cfg(target_os = "linux")]
            return libc::lgetxattr(path.as_ptr(), name.as_ptr(), buf, size);
        }
    }

    /// The attribute's value, `None` if the item has none by that name.
    pub fn get(path: &Path, name: &str) -> io::Result<Option<Vec<u8>>> {
        let (path, name) = cstrings(path, name)?;
        loop {
            // SAFETY: valid C strings; a null buffer of size 0 asks for the length.
            let len = unsafe { get_raw(&path, &name, std::ptr::null_mut(), 0) };
            if len < 0 {
                let e = last_error();
                return match e.raw_os_error() {
                    Some(NO_ATTR) => Ok(None),
                    _ => Err(e),
                };
            }
            let mut buf = vec![0u8; len as usize];
            // SAFETY: `buf` holds `buf.len()` bytes.
            let got = unsafe { get_raw(&path, &name, buf.as_mut_ptr().cast(), buf.len()) };
            if got >= 0 {
                buf.truncate(got as usize);
                return Ok(Some(buf));
            }
            let e = last_error();
            match e.raw_os_error() {
                Some(libc::ERANGE) => continue, // it grew in between
                Some(NO_ATTR) => return Ok(None),
                _ => return Err(e),
            }
        }
    }

    pub fn set(path: &Path, name: &str, value: &[u8]) -> io::Result<()> {
        let (path, name) = cstrings(path, name)?;
        // SAFETY: valid C strings and `value` of `value.len()` bytes.
        let rc = unsafe {
            #[cfg(target_os = "macos")]
            let rc = libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
                libc::XATTR_NOFOLLOW,
            );
            #[cfg(target_os = "linux")]
            let rc = libc::lsetxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
            );
            rc
        };
        if rc < 0 { Err(last_error()) } else { Ok(()) }
    }

    /// Removes the attribute; one that isn't there is no error.
    pub fn remove(path: &Path, name: &str) -> io::Result<()> {
        let (path, name) = cstrings(path, name)?;
        // SAFETY: valid C strings.
        let rc = unsafe {
            #[cfg(target_os = "macos")]
            let rc = libc::removexattr(path.as_ptr(), name.as_ptr(), libc::XATTR_NOFOLLOW);
            #[cfg(target_os = "linux")]
            let rc = libc::lremovexattr(path.as_ptr(), name.as_ptr());
            rc
        };
        if rc < 0 {
            let e = last_error();
            if e.raw_os_error() == Some(NO_ATTR) {
                return Ok(());
            }
            return Err(e);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(list: &[(&str, u8)]) -> Tags {
        Tags::new(list.iter().map(|(n, c)| Tag::new(*n, *c)))
    }

    #[test]
    fn new_sorts_and_keeps_one_per_name() {
        let t = tags(&[("b", 1), ("a", 0), ("b", 6), ("", 2)]);
        assert_eq!(t, Tags(vec![Tag::new("a", 0), Tag::new("b", 1)]));
        assert_eq!(t.get("b").map(|t| t.colour), Some(1));
        assert!(t.get("c").is_none());
    }

    #[test]
    fn merge_adds_and_removes_from_both_sides() {
        let base = tags(&[("keep", 0), ("gone-here", 0), ("gone-there", 0)]);
        let local = tags(&[("keep", 0), ("gone-there", 0), ("new-here", 0)]);
        let remote = tags(&[("keep", 0), ("gone-here", 0), ("new-there", 4)]);
        assert_eq!(
            Tags::merge(&base, &local, &remote),
            tags(&[("keep", 0), ("new-here", 0), ("new-there", 4)])
        );
    }

    #[test]
    fn merge_takes_a_colour_from_the_side_that_changed_it() {
        let base = tags(&[("a", 1), ("b", 1), ("c", 1)]);
        let local = tags(&[("a", 2), ("b", 1), ("c", 3)]);
        let remote = tags(&[("a", 1), ("b", 5), ("c", 4)]);
        // Both changed `c`: the remote wins, as for moves.
        assert_eq!(
            Tags::merge(&base, &local, &remote),
            tags(&[("a", 2), ("b", 5), ("c", 4)])
        );
        // Added on both sides in different colours.
        let none = Tags::default();
        assert_eq!(
            Tags::merge(&none, &tags(&[("x", 2)]), &tags(&[("x", 6)])),
            tags(&[("x", 6)])
        );
    }

    #[test]
    fn fill_keeps_what_the_device_cannot_hold() {
        let reference = tags(&[("Red", 6), ("a,b", 0), ("Work", 4)]);
        // Windows: nothing read, the reference stands.
        assert_eq!(Tags::fill(None, &reference, Os::Windows), reference);
        // Linux reads names only; `a,b` it can't hold at all.
        let read = tags(&[("Red", 0), ("Work", 0), ("New", 0)]);
        assert_eq!(
            Tags::fill(Some(&read), &reference, Os::Linux),
            tags(&[("New", 0), ("Red", 6), ("a,b", 0), ("Work", 4)])
        );
        // A tag removed on Linux stays removed.
        let read = tags(&[("Work", 0)]);
        assert_eq!(
            Tags::fill(Some(&read), &reference, Os::Linux),
            tags(&[("a,b", 0), ("Work", 4)])
        );
        // macOS holds everything: what it read is what it has.
        let read = tags(&[("Red", 2)]);
        assert_eq!(Tags::fill(Some(&read), &reference, Os::MacOs), read);
    }

    #[test]
    fn finder_round_trip() {
        let t = tags(&[("Red", 6), ("Work", 0), ("Ünïcode", 4)]);
        assert_eq!(decode_finder(&encode_finder(&t)), Some(t));
        // A name with a newline can't be held.
        let odd = tags(&[("a\nb", 1), ("c", 0)]);
        assert_eq!(decode_finder(&encode_finder(&odd)), Some(tags(&[("c", 0)])));
    }

    /// Tags set through Apple's own API (what Finder uses) are read here, and ours are read
    /// there.
    #[cfg(target_os = "macos")]
    #[test]
    fn finder_reads_and_writes_the_same_tags() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("f");
        std::fs::write(&file, b"x")?;
        let jxa = |script: String| -> io::Result<String> {
            let out = std::process::Command::new("osascript")
                .args(["-l", "JavaScript", "-e", &script])
                .output()?;
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
        };
        let url = format!("$.NSURL.fileURLWithPath({:?})", file.display().to_string());
        jxa(format!(
            "ObjC.import('Foundation'); \
             {url}.setResourceValueForKeyError($(['Work', 'Ünïcode']), $.NSURLTagNamesKey, null)"
        ))?;
        let names: Vec<String> = read(&file)
            .unwrap_or_default()
            .iter()
            .map(|t| t.name.clone())
            .collect();
        assert_eq!(names, ["Work", "Ünïcode"]);
        write(&file, &tags(&[("Blue", 4), ("Plain", 0)]))?;
        let shown = jxa(format!(
            "ObjC.import('Foundation'); var v = Ref(); \
             {url}.getResourceValueForKeyError(v, $.NSURLTagNamesKey, null); \
             ObjC.deepUnwrap(v[0]).sort().join(',')"
        ))?;
        assert_eq!(shown, "Blue,Plain");
        Ok(())
    }

    #[test]
    fn xdg_round_trip() {
        let t = tags(&[("Red", 6), ("work", 0), ("a,b", 0)]);
        assert_eq!(encode_xdg(&t), b"Red,work");
        assert_eq!(decode_xdg(b"Red,work,,"), tags(&[("Red", 0), ("work", 0)]));
    }

    #[test]
    fn too_many_tags_dont_fit() {
        let many = Tags::new((0..200).map(|i| Tag::new(format!("tag {i}"), 0)));
        assert!(!many.fits());
        assert!(tags(&[("Red", 6)]).fits());
    }

    /// On a system with tags, they're written, read back and removed; elsewhere nothing is.
    #[test]
    fn write_and_read_back() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("f");
        std::fs::write(&file, b"x")?;
        let t = tags(&[("Red", 6), ("Work", 0)]);
        let before = crate::stat(&file)?;
        std::thread::sleep(std::time::Duration::from_millis(20)); // past a coarse clock tick
        let got = write(&file, &t)?;
        if !CAN_TAG {
            assert_eq!((got, read(&file)), (None, None));
            return Ok(());
        }
        let Some(got) = got else {
            // A temp folder on a file system without attributes (tmpfs without user xattrs).
            eprintln!("no tags on this file system; skipped");
            return Ok(());
        };
        let held = Tags::fill(Some(&got), &t, Os::current());
        assert_eq!(held, t);
        // The scan notices a tag change by the change time; the bytes' fingerprint stays.
        let after = crate::stat(&file)?;
        assert_ne!(before.ctime_ns, after.ctime_ns);
        assert!(before.fingerprint().same_after_rename(&after));
        assert_eq!(read(&file), Some(got));
        assert_eq!(write(&file, &Tags::default())?, Some(Tags::default()));
        assert_eq!(read(&file), Some(Tags::default()));
        // A folder takes them too.
        assert_eq!(
            write(dir.path(), &t)?.map(|g| Tags::fill(Some(&g), &t, Os::current())),
            Some(t)
        );
        Ok(())
    }
}
