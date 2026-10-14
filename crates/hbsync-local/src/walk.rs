//! Tier-C catch-up: a depth-first walk of the sync root in [`RelPath`] order.
//!
//! Siblings are visited in byte order of their names and a folder comes right before its
//! contents, which is the order the scan reads the state DB's `local` tree in (depth first,
//! children by name). That lets [`crate::scan`] merge the two in one streaming pass. Memory is the listings of the folders on
//! the current path, not the whole tree.

use std::io;
use std::path::{Path, PathBuf};

use hbsync_platform::{EntryKind, LocalMeta, RelPath};

use hbsync_platform::listdir::{DirItem, list_dir};

use crate::filter::{Filter, Verdict};

/// Something the walk found.
#[derive(Debug)]
pub enum WalkEvent {
    /// A file or folder that can be synced.
    Entry { path: RelPath, meta: LocalMeta },
    /// A folder whose contents couldn't be listed. Its subtree is unknown, which is not the
    /// same as empty: nothing below it may be treated as deleted.
    Unreadable { path: RelPath, error: io::Error },
    /// An entry that is never synced.
    Skipped { path: PathBuf, reason: SkipReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    Symlink,
    /// Device, socket, FIFO.
    Special,
    /// Not valid UTF-8 (possible on Linux), so it has no remote spelling.
    NotUnicode,
    /// Not a valid name for the remote (for example more than 255 UTF-8 bytes).
    InvalidName,
    /// The client's own `.hbsync` folder.
    Excluded,
    /// On the built-in junk list: disposable.
    Junk,
    /// Matched by the user's ignore rules: kept on this device only.
    Ignored,
}

/// Decides whether the walk enters a folder.
pub type DescendFn<'a> = Box<dyn FnMut(&RelPath, &LocalMeta) -> bool + 'a>;

/// A lazy depth-first walk.
pub struct Walker<'a> {
    root: PathBuf,
    filter: &'a Filter,
    stack: Vec<Frame>,
    queued: Vec<WalkEvent>,
    descend: Option<DescendFn<'a>>,
}

impl std::fmt::Debug for Walker<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Walker")
            .field("root", &self.root)
            .field("depth", &self.stack.len())
            .finish()
    }
}

#[derive(Debug)]
struct Frame {
    dir: RelPath,
    /// Remaining children, sorted descending so `pop` yields them in ascending order.
    items: Vec<DirItem>,
}

impl<'a> Walker<'a> {
    /// Walks everything below `root` that `filter` lets through.
    pub fn new(root: &Path, filter: &'a Filter) -> Self {
        Self::at(root, RelPath::root(), filter, None)
    }

    /// Walks below `start` (which itself is not returned). `descend` decides for each folder
    /// whether its contents are walked; `None` walks everything.
    pub fn at(
        root: &Path,
        start: RelPath,
        filter: &'a Filter,
        descend: Option<DescendFn<'a>>,
    ) -> Self {
        let mut walker = Walker {
            root: root.to_path_buf(),
            filter,
            stack: Vec::new(),
            queued: Vec::new(),
            descend,
        };
        walker.open(start);
        walker
    }

    fn open(&mut self, dir: RelPath) {
        match list_dir(&dir.to_native(&self.root)) {
            Ok(mut items) => {
                items.sort_by(|a, b| b.name.cmp(&a.name));
                self.stack.push(Frame { dir, items });
            }
            Err(error) => self.queued.push(WalkEvent::Unreadable { path: dir, error }),
        }
    }

    fn classify(&self, dir: &RelPath, item: DirItem) -> WalkEvent {
        let rel_native = || {
            let mut p: PathBuf = dir.components().collect();
            p.push(&item.name);
            p
        };
        let Some(name) = item.name.to_str() else {
            return WalkEvent::Skipped {
                path: rel_native(),
                reason: SkipReason::NotUnicode,
            };
        };
        let filtered =
            match self
                .filter
                .in_folder(dir.as_str(), name, item.meta.kind == EntryKind::Dir)
            {
                Verdict::Sync => None,
                Verdict::Excluded => Some(SkipReason::Excluded),
                Verdict::Junk => Some(SkipReason::Junk),
                Verdict::Ignored => Some(SkipReason::Ignored),
            };
        if let Some(reason) = filtered {
            return WalkEvent::Skipped {
                path: rel_native(),
                reason,
            };
        }
        let reason = match item.meta.kind {
            EntryKind::Symlink => Some(SkipReason::Symlink),
            EntryKind::Other => Some(SkipReason::Special),
            EntryKind::File | EntryKind::Dir => None,
        };
        if let Some(reason) = reason {
            return WalkEvent::Skipped {
                path: rel_native(),
                reason,
            };
        }
        match dir.join(name) {
            Ok(path) => WalkEvent::Entry {
                path,
                meta: item.meta,
            },
            Err(_) => WalkEvent::Skipped {
                path: rel_native(),
                reason: SkipReason::InvalidName,
            },
        }
    }
}

impl Iterator for Walker<'_> {
    type Item = WalkEvent;

    fn next(&mut self) -> Option<WalkEvent> {
        loop {
            if let Some(event) = self.queued.pop() {
                return Some(event);
            }
            let frame = self.stack.last_mut()?;
            let Some(item) = frame.items.pop() else {
                self.stack.pop();
                continue;
            };
            let dir = frame.dir.clone();
            let event = self.classify(&dir, item);
            if let WalkEvent::Entry { path, meta } = &event
                && meta.kind == EntryKind::Dir
                && self
                    .descend
                    .as_mut()
                    .is_none_or(|descend| descend(path, meta))
            {
                // Pre-order: the folder is returned now, its contents on the next calls.
                // `open` may queue an Unreadable event, which must come right after the folder.
                let path = path.clone();
                self.open(path);
            }
            return Some(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn entries(root: &Path) -> Vec<String> {
        Walker::new(root, &Filter::default())
            .filter_map(|e| match e {
                WalkEvent::Entry { path, .. } => Some(path.as_str().to_owned()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn depth_first_in_relpath_order() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let r = dir.path();
        for d in ["a", "a/b", "a-b", "z"] {
            fs::create_dir(r.join(d))?;
        }
        for f in ["a/b/c.txt", "a/z.txt", "a.txt", "a-b/x", "B"] {
            fs::write(r.join(f), b"x")?;
        }
        fs::create_dir(r.join(".hbsync"))?;
        fs::write(&crate::layout::older_db(r)[0], b"")?;

        let got = entries(r);
        assert_eq!(
            got,
            [
                "B",
                "a",
                "a/b",
                "a/b/c.txt",
                "a/z.txt",
                "a-b",
                "a-b/x",
                "a.txt",
                "z"
            ]
        );
        let mut sorted: Vec<RelPath> = got
            .iter()
            .map(|s| RelPath::new(s))
            .collect::<Result<_, _>>()
            .map_err(io::Error::other)?;
        sorted.sort();
        assert_eq!(sorted.iter().map(RelPath::as_str).collect::<Vec<_>>(), got);
        Ok(())
    }

    #[test]
    fn filtered_entries_are_skipped_and_not_entered() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let r = dir.path();
        for d in ["app", "app/node_modules", "app/node_modules/x", "._res"] {
            fs::create_dir(r.join(d))?;
        }
        for f in [
            "app/main.js",
            "app/.DS_Store",
            "app/node_modules/x/y.js",
            "._res/data",
            "THUMBS.DB",
            "~$report.docx",
            "report.docx",
        ] {
            fs::write(r.join(f), b"x")?;
        }
        let filter = Filter::from_rules("node_modules/\n", false).map_err(io::Error::other)?;
        let mut entries = Vec::new();
        let mut skipped = Vec::new();
        for event in Walker::new(r, &filter) {
            match event {
                WalkEvent::Entry { path, .. } => entries.push(path.as_str().to_owned()),
                WalkEvent::Skipped { path, reason } => skipped.push((path, reason)),
                WalkEvent::Unreadable { .. } => return Err(io::Error::other("unreadable")),
            }
        }
        assert_eq!(entries, ["app", "app/main.js", "report.docx"]);
        skipped.sort_by(|a, b| a.0.cmp(&b.0));
        let p = |s: &str| s.split('/').collect::<PathBuf>();
        assert_eq!(
            skipped,
            [
                (p("._res"), SkipReason::Junk),
                (p("THUMBS.DB"), SkipReason::Junk),
                (p("app/.DS_Store"), SkipReason::Junk),
                (p("app/node_modules"), SkipReason::Ignored),
                (p("~$report.docx"), SkipReason::Junk),
            ]
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_skipped_not_followed() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        fs::create_dir(dir.path().join("real"))?;
        fs::write(dir.path().join("real/f"), b"x")?;
        std::os::unix::fs::symlink("real", dir.path().join("link"))?;
        assert_eq!(entries(dir.path()), ["real", "real/f"]);
        let skipped: Vec<_> = Walker::new(dir.path(), &Filter::default())
            .filter_map(|e| match e {
                WalkEvent::Skipped { reason, .. } => Some(reason),
                _ => None,
            })
            .collect();
        assert_eq!(skipped, [SkipReason::Symlink]);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_folder_is_reported_after_itself() -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir()?;
        let locked = dir.path().join("locked");
        fs::create_dir(&locked)?;
        fs::write(locked.join("secret"), b"x")?;
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000))?;
        if fs::read_dir(&locked).is_ok() {
            // Running as root: permissions don't apply.
            fs::set_permissions(&locked, fs::Permissions::from_mode(0o755))?;
            return Ok(());
        }
        let events: Vec<_> = Walker::new(dir.path(), &Filter::default()).collect();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755))?;
        assert!(matches!(&events[0], WalkEvent::Entry { path, .. } if path.as_str() == "locked"));
        assert!(
            matches!(&events[1], WalkEvent::Unreadable { path, .. } if path.as_str() == "locked")
        );
        assert_eq!(events.len(), 2);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn non_unicode_names_are_skipped() -> io::Result<()> {
        use std::os::unix::ffi::OsStrExt;
        let dir = tempfile::tempdir()?;
        let name = std::ffi::OsStr::from_bytes(b"bad\xff");
        fs::write(dir.path().join(name), b"x")?;
        let events: Vec<_> = Walker::new(dir.path(), &Filter::default()).collect();
        assert!(matches!(
            events[..],
            [WalkEvent::Skipped {
                reason: SkipReason::NotUnicode,
                ..
            }]
        ));
        Ok(())
    }
}
