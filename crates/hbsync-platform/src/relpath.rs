//! Paths relative to the sync root.
//!
//! A [`RelPath`] is a `/`-separated list of local names (the names as they exist on disk, which
//! may be escaped by the name map). The empty path is the root itself.
//!
//! Ordering is component-wise (`"a/b" < "a-b"` because component `"a"` < `"a-b"`), so a
//! depth-first walk that visits siblings in byte order yields paths in [`RelPath`] order. The
//! walker's streaming merge against the state DB depends on that: the scan reads the DB's
//! tree depth first with children sorted the same way.

use std::cmp::Ordering;
use std::fmt;
use std::path::{Path, PathBuf};

use crate::name::{NameIssue, check_remote_name};

/// A validated path relative to the sync root.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct RelPath(String);

impl RelPath {
    /// The sync root.
    pub const fn root() -> Self {
        RelPath(String::new())
    }

    /// Parses a `/`-separated relative path. Every component must be a valid name.
    pub fn new(path: &str) -> Result<Self, NameIssue> {
        if path.is_empty() {
            return Ok(Self::root());
        }
        for component in path.split('/') {
            check_remote_name(component)?;
        }
        Ok(RelPath(path.to_owned()))
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The components, outermost first. Empty for the root.
    pub fn components(&self) -> impl DoubleEndedIterator<Item = &str> {
        self.0.split('/').filter(|c| !c.is_empty())
    }

    pub fn depth(&self) -> usize {
        self.components().count()
    }

    /// The last component, or `None` for the root.
    pub fn name(&self) -> Option<&str> {
        if self.is_root() {
            None
        } else {
            Some(self.0.rsplit('/').next().unwrap_or(&self.0))
        }
    }

    /// The containing folder, or `None` for the root.
    pub fn parent(&self) -> Option<RelPath> {
        if self.is_root() {
            return None;
        }
        Some(match self.0.rfind('/') {
            Some(i) => RelPath(self.0[..i].to_owned()),
            None => RelPath::root(),
        })
    }

    /// Appends one component.
    pub fn join(&self, name: &str) -> Result<RelPath, NameIssue> {
        check_remote_name(name)?;
        Ok(if self.is_root() {
            RelPath(name.to_owned())
        } else {
            RelPath(format!("{}/{}", self.0, name))
        })
    }

    /// `true` if `self` is `other` or lies below it.
    pub fn starts_with(&self, other: &RelPath) -> bool {
        other.is_root()
            || self.0 == other.0
            || (self.0.starts_with(&other.0) && self.0.as_bytes().get(other.0.len()) == Some(&b'/'))
    }

    /// The absolute native path below `root`, with the Windows long-path prefix where needed.
    pub fn to_native(&self, root: &Path) -> PathBuf {
        let mut path = root.to_path_buf();
        for component in self.components() {
            path.push(component);
        }
        crate::winpath::extended(&path)
    }
}

/// Component-wise comparison of two `/`-separated paths: `/` sorts before every other byte.
pub fn path_cmp(a: &str, b: &str) -> Ordering {
    let rank = |byte: u8| {
        if byte == b'/' {
            0u16
        } else {
            u16::from(byte) + 1
        }
    };
    let (a, b) = (a.as_bytes(), b.as_bytes());
    for (&x, &y) in a.iter().zip(b) {
        match rank(x).cmp(&rank(y)) {
            Ordering::Equal => {}
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

impl Ord for RelPath {
    fn cmp(&self, other: &Self) -> Ordering {
        path_cmp(&self.0, &other.0)
    }
}

impl PartialOrd for RelPath {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for RelPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RelPath({:?})", self.0)
    }
}

impl fmt::Display for RelPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_root() {
            f.write_str("/")
        } else {
            f.write_str(&self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn p(s: &str) -> RelPath {
        RelPath::new(s).expect("valid path")
    }

    #[test]
    fn parse_and_navigate() {
        let path = p("a/b/c.txt");
        assert_eq!(path.components().collect::<Vec<_>>(), ["a", "b", "c.txt"]);
        assert_eq!(path.name(), Some("c.txt"));
        assert_eq!(path.parent(), Some(p("a/b")));
        assert_eq!(p("a").parent(), Some(RelPath::root()));
        assert_eq!(RelPath::root().parent(), None);
        assert_eq!(RelPath::root().join("x").map(|j| j.0), Ok("x".into()));
        assert_eq!(path.depth(), 3);
    }

    #[test]
    fn rejects_bad_components() {
        assert!(RelPath::new("a//b").is_err());
        assert!(RelPath::new("/a").is_err());
        assert!(RelPath::new("a/../b").is_err());
        assert!(RelPath::new("a/").is_err());
    }

    #[test]
    fn prefix_is_component_wise() {
        assert!(p("a/b").starts_with(&p("a")));
        assert!(p("a").starts_with(&p("a")));
        assert!(!p("ab").starts_with(&p("a")));
        assert!(p("a").starts_with(&RelPath::root()));
    }

    #[test]
    fn order_is_depth_first() {
        let mut paths = [p("a-b"), p("a/b"), p("a"), p("a/b/c"), p("a.txt"), p("b")];
        paths.sort();
        let sorted: Vec<_> = paths.iter().map(RelPath::as_str).collect();
        assert_eq!(sorted, ["a", "a/b", "a/b/c", "a-b", "a.txt", "b"]);
    }

    proptest! {
        #[test]
        fn order_matches_component_order(
            a in proptest::collection::vec("[a-c.-]{1,3}", 1..4),
            b in proptest::collection::vec("[a-c.-]{1,3}", 1..4),
        ) {
            let valid = |v: &Vec<String>| v.iter().all(|c| check_remote_name(c).is_ok());
            prop_assume!(valid(&a) && valid(&b));
            let (pa, pb) = (p(&a.join("/")), p(&b.join("/")));
            prop_assert_eq!(pa.cmp(&pb), a.cmp(&b));
        }
    }
}
